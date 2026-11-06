import json
import logging

import pytest
from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import config, errors, message
from tests.support import consumers, iam, sqs
from tests.support import events as hook_events

RECEIVE = "sqs:ReceiveMessage"
DELETE = "sqs:DeleteMessage"
CHANGE_VISIBILITY = "sqs:ChangeMessageVisibility"


def _record_visibility_changes(client: sqs_client.SQSClient) -> dict[str, int]:
    """Receipt handle -> the visibility timeout the consumer last gave it."""
    changes: dict[str, int] = {}

    def record(params: dict[str, bytes], **_: object) -> None:
        for entry in json.loads(params["body"])["Entries"]:
            changes[entry["ReceiptHandle"]] = entry["VisibilityTimeout"]

    client.meta.events.register("before-call.sqs.ChangeMessageVisibilityBatch", record)
    return changes


def _delays_by_body(
    events: hook_events.Events, changes: dict[str, int]
) -> dict[str, int]:
    return {
        m.body: changes[m.receipt_handle]
        for _, group in events.failures
        for m in group
        if m.receipt_handle in changes
    }


class TestRetryAfter:
    @pytest.mark.parametrize("delay", [-1, 43201])
    def test_rejects_a_delay_sqs_would_refuse(self, delay):
        with pytest.raises(ValueError, match="0..43200"):
            errors.RetryAfterError(delay)

    async def test_works_without_release_on_error(self, client: sqs_client.SQSClient):
        queue_url = sqs.create_queue(client, VisibilityTimeout="60")
        consumers.send(client, queue_url, "retry-me")
        attempts: list[str] = []

        async def retry_once(item: message.Message) -> None:
            attempts.append(item.body)
            if len(attempts) == 1:
                raise errors.RetryAfterError(0)

        async with consumers.build(queue_url, client=client, handler=retry_once):
            # Far below the 60s visibility timeout: only the retry gets here.
            await sqs.wait_for(lambda: sqs.depth(client, queue_url) == (0, 0))

        assert attempts == ["retry-me", "retry-me"]

    async def test_overrides_release_on_error_for_its_own_message(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "plain", "retry")
        changes = _record_visibility_changes(client)
        events = hook_events.Events()

        async def handle(item: message.Message) -> None:
            if item.body == "retry":
                raise errors.RetryAfterError(30)
            raise ValueError("plain failure")

        async with consumers.build(
            queue_url,
            client=client,
            handler=handle,
            hooks=events.hooks(),
            release_on_error=config.ReleaseOnError(visibility_timeout=5),
        ):
            await sqs.wait_for(lambda: len(changes) == 2)

        assert _delays_by_body(events, changes) == {"plain": 5, "retry": 30}

    async def test_is_logged_at_info_and_still_reaches_the_error_hook(
        self,
        client: sqs_client.SQSClient,
        queue_url: str,
        caplog: pytest.LogCaptureFixture,
    ):
        consumers.send(client, queue_url, "a")
        events = hook_events.Events()

        async def retry(item: message.Message) -> None:
            raise errors.RetryAfterError(30)

        with caplog.at_level(logging.INFO, logger="sqs_consumer"):
            async with consumers.build(
                queue_url, client=client, handler=retry, hooks=events.hooks()
            ):
                await sqs.wait_for(lambda: events.failures)

        [(error, _)] = events.failures
        assert isinstance(error, errors.RetryAfterError)
        assert error.delay == 30
        [record] = [r for r in caplog.records if "asked to retry" in r.message]
        assert record.levelno == logging.INFO
        assert events.errors == []

    async def test_a_batch_handler_raising_it_delays_the_whole_batch(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a", "b")
        changes = _record_visibility_changes(client)
        events = hook_events.Events()

        async def retry_all(items):
            raise errors.RetryAfterError(20)

        subject = consumers.build_batch(
            queue_url, retry_all, client=client, hooks=events.hooks()
        )
        async with subject:
            await sqs.wait_for(lambda: len(changes) == 2)

        assert _delays_by_body(events, changes) == {"a": 20, "b": 20}

    async def test_a_batch_failure_can_give_one_message_its_own_delay(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "ok", "later", "broken")
        changes = _record_visibility_changes(client)
        events = hook_events.Events()

        async def picky(items):
            by_body = {item.body: item for item in items}
            raise errors.BatchFailureError(
                {
                    by_body["later"]: errors.RetryAfterError(10),
                    by_body["broken"]: ValueError("broken"),
                }
            )

        subject = consumers.build_batch(
            queue_url, picky, client=client, hooks=events.hooks()
        )
        async with subject:
            await sqs.wait_for(lambda: len(events.failures) == 2)

        # Without release_on_error the plain failure is left alone.
        assert _delays_by_body(events, changes) == {"later": 10}
        assert events.bodies(events.processed) == ["ok"]

    async def test_the_skipped_rest_of_a_fifo_group_takes_the_same_delay(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, fifo=True, VisibilityTimeout="60")
        for body in ["0", "1", "2"]:
            client.send_message(
                QueueUrl=queue_url, MessageBody=body, MessageGroupId="orders"
            )
        changes = _record_visibility_changes(client)

        async def retry_first(item: message.Message) -> None:
            raise errors.RetryAfterError(15)

        async with consumers.build(queue_url, client=client, handler=retry_first):
            await sqs.wait_for(lambda: len(changes) == 3)

        assert sorted(changes.values()) == [15, 15, 15]

    async def test_a_denied_retry_is_reported_naming_retry_after(
        self,
        client: sqs_client.SQSClient,
        queue_url: str,
        iam_enforcement: iam.IamEnforcement,
    ):
        consumers.send(client, queue_url, "a")
        limited = iam.create_limited_client([RECEIVE, DELETE])
        events = hook_events.Events()

        async def retry(item: message.Message) -> None:
            raise errors.RetryAfterError(0)

        iam_enforcement.enable()
        async with consumers.build(
            queue_url, client=limited, handler=retry, hooks=events.hooks()
        ):
            await sqs.wait_for(lambda: events.errors)
        iam_enforcement.disable()

        [error] = events.errors
        assert isinstance(error, errors.MissingPermissionError)
        assert (error.action, error.feature) == (CHANGE_VISIBILITY, "RetryAfterError")
        assert "stop raising errors.RetryAfterError" in str(error)
