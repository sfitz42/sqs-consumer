import asyncio
import dataclasses
import json
import logging

import boto3
import botocore.exceptions
import botocore.stub
import pytest
from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import config, consumer, errors, hooks, message
from tests.fixtures import sqs_messages
from tests.support import consumers, iam, sqs
from tests.support import events as hook_events

RECEIVE = "sqs:ReceiveMessage"
DELETE = "sqs:DeleteMessage"
CHANGE_VISIBILITY = "sqs:ChangeMessageVisibility"
GET_QUEUE_ATTRIBUTES = "sqs:GetQueueAttributes"


class TestConstruction:
    def test_the_handler_can_be_passed_positionally(self):
        assert consumer.Consumer("q", consumers.accept)._handler is consumers.accept

    @pytest.mark.parametrize(
        ("option", "value"),
        [
            ("io_threads", 0),
            ("handler_timeout", 0),
            ("shutdown_timeout", -1),
        ],
    )
    def test_rejects_out_of_range_options(self, option, value):
        with pytest.raises(errors.ConfigurationError, match=option):
            consumer.Consumer("q", handler=consumers.accept, **{option: value})

    def test_every_option_has_a_setting(self):
        settings = {field.name for field in dataclasses.fields(consumer._Settings)}

        assert set(consumer.ConsumerOptions.__annotations__) == settings

    def test_rejects_a_heartbeat_that_would_outlast_the_receive_visibility(self):
        with pytest.raises(errors.ConfigurationError, match="receive visibility"):
            consumer.Consumer(
                "q",
                handler=consumers.accept,
                receive=config.Receive(visibility_timeout=10),
                heartbeat=config.Heartbeat(interval=10.5, visibility_timeout=60),
            )


class TestRequiredIamActions:
    def test_defaults_need_only_receive_and_delete(self):
        assert consumers.build("q").required_iam_actions() == {RECEIVE, DELETE}

    def test_a_receive_only_consumer_needs_no_delete(self):
        needed = consumers.build("q", delete_on_success=False).required_iam_actions()

        assert needed == {RECEIVE}

    def test_heartbeat_adds_change_message_visibility(self):
        heartbeat = config.Heartbeat(interval=5, visibility_timeout=30)

        needed = consumers.build("q", heartbeat=heartbeat).required_iam_actions()

        assert needed == {RECEIVE, DELETE, CHANGE_VISIBILITY}

    def test_release_on_error_adds_change_message_visibility(self):
        needed = consumers.build(
            "q", release_on_error=config.ReleaseOnError()
        ).required_iam_actions()

        assert needed == {RECEIVE, DELETE, CHANGE_VISIBILITY}

    def test_sns_unwrapping_needs_no_extra_actions(self):
        needed = consumers.build("q", unwrap_sns=True).required_iam_actions()

        assert needed == {RECEIVE, DELETE}

    def test_queue_check_adds_get_queue_attributes(self):
        needed = consumers.build("q", check_queue=True).required_iam_actions()

        assert needed == {RECEIVE, DELETE, GET_QUEUE_ATTRIBUTES}


class TestIamOptIns:
    async def test_defaults_only_call_receive_and_delete(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a")
        operations = sqs.record_operations(client)
        events = hook_events.Events()

        async with consumers.build(queue_url, client=client, hooks=events.hooks()):
            await sqs.wait_for(lambda: events.processed)

        assert set(operations) == {"ReceiveMessage", "DeleteMessageBatch"}

    async def test_opted_in_features_call_their_actions(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a")
        operations = sqs.record_operations(client)
        events = hook_events.Events()

        async def slow(item: message.Message) -> None:
            await asyncio.sleep(0.3)

        async with consumers.build(
            queue_url,
            client=client,
            handler=slow,
            hooks=events.hooks(),
            heartbeat=config.Heartbeat(interval=0.1, visibility_timeout=5),
            check_queue=True,
        ):
            await sqs.wait_for(lambda: events.processed)

        assert "GetQueueAttributes" in operations
        assert "ChangeMessageVisibilityBatch" in operations

    async def test_receive_only_never_deletes(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a")
        operations = sqs.record_operations(client)
        events = hook_events.Events()

        async with consumers.build(
            queue_url, client=client, hooks=events.hooks(), delete_on_success=False
        ):
            await sqs.wait_for(lambda: events.processed)

        assert "DeleteMessageBatch" not in operations
        assert sqs.depth(client, queue_url) == (0, 1)


class TestMissingPermissions:
    """Moto enforces IAM, so a limited user gets a real AccessDenied."""

    async def test_a_denied_heartbeat_is_reported_once_and_processing_continues(
        self,
        client: sqs_client.SQSClient,
        queue_url: str,
        iam_enforcement: iam.IamEnforcement,
    ):
        consumers.send(client, queue_url, "a")
        limited = iam.create_limited_client([RECEIVE, DELETE])
        events = hook_events.Events()

        async def slow(item: message.Message) -> None:
            await asyncio.sleep(0.5)

        iam_enforcement.enable()
        async with consumers.build(
            queue_url,
            client=limited,
            handler=slow,
            hooks=events.hooks(),
            heartbeat=config.Heartbeat(interval=0.1, visibility_timeout=5),
        ) as running:
            await sqs.wait_for(lambda: events.processed)
            assert running.is_running
        iam_enforcement.disable()

        [error] = events.errors
        assert isinstance(error, errors.MissingPermissionError)
        assert (error.action, error.feature) == (CHANGE_VISIBILITY, "heartbeat")
        assert "heartbeat=None" in str(error)
        assert sqs.depth(client, queue_url) == (0, 0)

    async def test_a_denied_release_is_reported_once_across_failures(
        self,
        client: sqs_client.SQSClient,
        queue_url: str,
        iam_enforcement: iam.IamEnforcement,
    ):
        consumers.send(client, queue_url, "first")
        limited = iam.create_limited_client([RECEIVE, DELETE])
        events = hook_events.Events()

        async def fail(item: message.Message) -> None:
            raise ValueError(item.body)

        iam_enforcement.enable()
        async with consumers.build(
            queue_url,
            client=limited,
            handler=fail,
            hooks=events.hooks(),
            release_on_error=config.ReleaseOnError(),
        ):
            await sqs.wait_for(lambda: len(events.failures) == 1)
            iam_enforcement.disable()
            consumers.send(client, queue_url, "second")
            iam_enforcement.enable()
            await sqs.wait_for(lambda: len(events.failures) == 2)
        iam_enforcement.disable()

        [error] = events.errors
        assert isinstance(error, errors.MissingPermissionError)
        assert (error.action, error.feature) == (CHANGE_VISIBILITY, "release_on_error")

    async def test_a_denied_fifo_release_keeps_the_whole_group_in_flight(
        self, client: sqs_client.SQSClient, iam_enforcement: iam.IamEnforcement
    ):
        """Nothing of the group is released early, so it all comes back together
        after the visibility timeout."""
        queue_url = sqs.create_queue(client, fifo=True, VisibilityTimeout="60")
        for body in ["0", "1", "2"]:
            client.send_message(
                QueueUrl=queue_url, MessageBody=body, MessageGroupId="orders"
            )
        limited = iam.create_limited_client([RECEIVE, DELETE])
        events = hook_events.Events()

        async def fail_first(item: message.Message) -> None:
            if item.body == "0":
                raise ValueError(item.body)

        iam_enforcement.enable()
        async with consumers.build(
            queue_url,
            client=limited,
            handler=fail_first,
            hooks=events.hooks(),
            release_on_error=config.ReleaseOnError(),
        ) as running:
            await sqs.wait_for(lambda: events.errors)
            assert running.is_running
        iam_enforcement.disable()

        [error] = events.errors
        assert isinstance(error, errors.MissingPermissionError)
        assert (error.action, error.feature) == (CHANGE_VISIBILITY, "release_on_error")
        [(_, failed)] = events.failures
        assert [m.body for m in failed] == ["0", "1", "2"]
        assert events.processed == []
        assert sqs.depth(client, queue_url) == (0, 3)

    async def test_a_denied_delete_is_reported_and_the_consumer_keeps_running(
        self,
        client: sqs_client.SQSClient,
        queue_url: str,
        iam_enforcement: iam.IamEnforcement,
    ):
        consumers.send(client, queue_url, "a")
        limited = iam.create_limited_client([RECEIVE])
        events = hook_events.Events()

        iam_enforcement.enable()
        async with consumers.build(
            queue_url, client=limited, hooks=events.hooks()
        ) as running:
            await sqs.wait_for(lambda: events.errors)
            assert running.is_running
        iam_enforcement.disable()

        error = events.errors[0]
        assert isinstance(error, errors.MissingPermissionError)
        assert (error.action, error.feature) == (DELETE, "delete_on_success")

    async def test_a_denied_queue_check_fails_start(
        self,
        client: sqs_client.SQSClient,
        queue_url: str,
        iam_enforcement: iam.IamEnforcement,
    ):
        limited = iam.create_limited_client([RECEIVE, DELETE])
        subject = consumers.build(queue_url, client=limited, check_queue=True)

        iam_enforcement.enable()
        with pytest.raises(errors.MissingPermissionError, match="GetQueueAttributes"):
            await subject.start()

        assert not subject.is_running


class TestQueueCheck:
    async def test_rejects_a_heartbeat_that_outlasts_the_queues_visibility_timeout(
        self, client: sqs_client.SQSClient
    ):
        short = sqs.create_queue(client, VisibilityTimeout="2")
        subject = consumers.build(
            short,
            client=client,
            check_queue=True,
            heartbeat=config.Heartbeat(interval=5, visibility_timeout=30),
        )

        with pytest.raises(errors.ConfigurationError, match="default visibility"):
            await subject.start()

    async def test_accepts_a_heartbeat_inside_the_queues_visibility_timeout(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        subject = consumers.build(
            queue_url,
            client=client,
            check_queue=True,
            heartbeat=config.Heartbeat(interval=5, visibility_timeout=30),
        )

        async with subject:
            assert subject.is_running

    async def test_a_missing_queue_fails_start(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        subject = consumers.build(
            queue_url + "-missing", client=client, check_queue=True
        )

        with pytest.raises(botocore.exceptions.ClientError, match="exist"):
            await subject.start()

        assert not subject.is_running


class TestHandlers:
    async def test_one_failing_message_does_not_stop_its_siblings(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "good", "bad")
        events = hook_events.Events()

        async def picky(item: message.Message) -> None:
            if item.body == "bad":
                raise ValueError("boom")

        async with consumers.build(
            queue_url, client=client, handler=picky, hooks=events.hooks()
        ):
            await sqs.wait_for(lambda: events.processed and events.failures)

        [(error, failed)] = events.failures
        assert str(error) == "boom"
        assert events.bodies(failed) == ["bad"]
        assert events.bodies(events.processed) == ["good"]
        assert sqs.depth(client, queue_url) == (0, 1)

    async def test_a_handler_that_exceeds_its_timeout_fails_and_is_not_deleted(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "slow")
        events = hook_events.Events()

        async def forever(item: message.Message) -> None:
            await asyncio.sleep(30)

        async with consumers.build(
            queue_url,
            client=client,
            handler=forever,
            handler_timeout=0.1,
            hooks=events.hooks(),
        ):
            await sqs.wait_for(lambda: events.failures)

        [(error, _)] = events.failures
        assert isinstance(error, errors.HandlerTimeoutError)
        assert events.processed == []
        assert sqs.depth(client, queue_url) == (0, 1)

    async def test_a_batch_handler_returning_none_acks_every_message(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a", "b", "c")
        seen: list[int] = []

        async def whole_batch(items):
            seen.append(len(items))

        subject = consumers.build_batch(queue_url, whole_batch, client=client)
        async with subject:
            await sqs.wait_for(lambda: sqs.depth(client, queue_url) == (0, 0))

        assert sum(seen) == 3

    async def test_a_raising_batch_handler_fails_the_whole_batch(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a", "b")
        events = hook_events.Events()

        async def broken(items):
            raise RuntimeError("batch boom")

        subject = consumers.build_batch(
            queue_url, broken, client=client, hooks=events.hooks()
        )
        async with subject:
            await sqs.wait_for(lambda: events.failures)

        [(error, failed)] = events.failures
        assert str(error) == "batch boom"
        assert events.bodies(failed) == ["a", "b"]
        assert sqs.depth(client, queue_url) == (0, 2)

    async def test_a_batch_failure_keeps_what_it_names_and_acks_the_rest(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a", "b", "c")
        events = hook_events.Events()

        async def picky(items):
            raise errors.BatchFailureError(
                {item: ValueError(item.body) for item in items if item.body != "a"}
            )

        subject = consumers.build_batch(
            queue_url, picky, client=client, hooks=events.hooks()
        )
        async with subject:
            await sqs.wait_for(lambda: events.processed and events.failures)

        assert events.bodies(events.processed) == ["a"]
        assert sorted(str(error) for error, _ in events.failures) == ["b", "c"]
        assert all(len(failed) == 1 for _, failed in events.failures)
        assert sqs.depth(client, queue_url) == (0, 2)


class TestHooks:
    async def test_a_failing_hook_is_logged_and_does_not_stop_processing(
        self,
        client: sqs_client.SQSClient,
        queue_url: str,
        caplog: pytest.LogCaptureFixture,
    ):
        consumers.send(client, queue_url, "a", "b")
        processed: list[str] = []

        async def explode(item: message.Message) -> None:
            raise RuntimeError("hook bug")

        async def explode_async(item: message.Message) -> None:
            raise RuntimeError("async hook bug")

        async def record(item: message.Message) -> None:
            processed.append(item.body)

        subject = consumers.build(
            queue_url,
            client=client,
            handler=record,
            hooks=hooks.Hooks(
                on_message_received=explode, on_message_processed=explode_async
            ),
        )
        with caplog.at_level(logging.ERROR):
            async with subject:
                await sqs.wait_for(lambda: len(processed) == 2)
                await sqs.wait_for(lambda: sqs.depth(client, queue_url) == (0, 0))

        assert sorted(processed) == ["a", "b"]
        assert "hook bug" in caplog.text
        assert "async hook bug" in caplog.text

    async def test_reports_an_empty_poll_and_the_stop(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        events = hook_events.Events()

        async with consumers.build(queue_url, client=client, hooks=events.hooks()):
            await sqs.wait_for(lambda: events.empty)

        assert events.empty >= 1
        assert events.stopped == 1


class TestReceiveErrors:
    async def test_errors_are_logged(
        self,
        client: sqs_client.SQSClient,
        queue_url: str,
        caplog: pytest.LogCaptureFixture,
    ):
        subject = consumers.build(queue_url + "-missing", client=client)

        with caplog.at_level(logging.WARNING, logger="sqs_consumer.consumer"):
            async with subject:
                await sqs.wait_for(lambda: "sqs consumer error" in caplog.text)

        assert "exist" in caplog.text

    async def test_go_to_the_logger_passed_in(
        self,
        client: sqs_client.SQSClient,
        queue_url: str,
        caplog: pytest.LogCaptureFixture,
    ):
        app_logger = logging.LoggerAdapter(
            logging.getLogger("app.orders"), {"service": "orders"}
        )
        subject = consumers.build(
            queue_url + "-missing", client=client, logger=app_logger
        )

        with caplog.at_level(logging.WARNING, logger="app.orders"):
            async with subject:
                await sqs.wait_for(lambda: "sqs consumer error" in caplog.text)

        record = next(r for r in caplog.records if "sqs consumer error" in r.message)
        assert record.name == "app.orders"
        assert record.__dict__["service"] == "orders"

    async def test_backs_off_instead_of_hot_looping_on_a_broken_queue(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        events = hook_events.Events()
        subject = consumers.build(
            queue_url + "-missing",
            client=client,
            hooks=events.hooks(),
            backoff=config.Backoff(initial=0.3, maximum=0.3, jitter=False),
        )

        async with subject:
            await asyncio.sleep(1.0)
            assert subject.is_running

        assert 2 <= len(events.errors) <= 5


class TestLifecycle:
    async def test_cannot_be_started_twice(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        async with consumers.build(queue_url, client=client) as running:
            with pytest.raises(RuntimeError, match="already running"):
                await running.start()

    async def test_reports_running_only_between_start_and_stop(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        subject = consumers.build(queue_url, client=client)

        before = subject.is_running
        await subject.start()
        during = subject.is_running
        await subject.stop()

        assert (before, during, subject.is_running) == (False, True, False)

    async def test_can_be_restarted_after_stopping(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        events = hook_events.Events()
        subject = consumers.build(queue_url, client=client, hooks=events.hooks())
        await subject.start()
        await subject.stop()
        consumers.send(client, queue_url, "after-restart")

        async with subject:
            await sqs.wait_for(lambda: events.processed)

        assert events.bodies(events.processed) == ["after-restart"]

    async def test_stop_waits_for_in_flight_handlers_and_acks_them(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a")
        started = asyncio.Event()
        events = hook_events.Events()

        async def slow(item: message.Message) -> None:
            started.set()
            await asyncio.sleep(0.5)

        subject = consumers.build(
            queue_url, client=client, handler=slow, hooks=events.hooks()
        )
        await subject.start()
        await started.wait()

        await subject.stop()

        assert events.bodies(events.processed) == ["a"]
        assert sqs.depth(client, queue_url) == (0, 0)

    async def test_stop_timeout_cancels_handlers_and_deletes_nothing(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a")
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def stuck(item: message.Message) -> None:
            started.set()
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        subject = consumers.build(queue_url, client=client, handler=stuck)
        await subject.start()
        await started.wait()

        await subject.stop(timeout=0.2)

        assert cancelled.is_set()
        assert not subject.is_running
        assert sqs.depth(client, queue_url) == (0, 1)

    async def test_a_poll_abandoned_by_stop_releases_what_it_later_receives(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        subject = consumers.build(
            queue_url,
            client=client,
            receive=config.Receive(wait_time_seconds=3),
            release_on_error=config.ReleaseOnError(),
        )
        await subject.start()
        await asyncio.sleep(0.3)
        await subject.stop(timeout=0.1)

        consumers.send(client, queue_url, "late")

        redelivered = await sqs.receive_eventually(client, queue_url)
        assert redelivered["Body"] == "late"
        assert redelivered["Attributes"]["ApproximateReceiveCount"] == "2"

    async def test_without_release_an_abandoned_polls_message_stays_in_flight(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        subject = consumers.build(
            queue_url, client=client, receive=config.Receive(wait_time_seconds=3)
        )
        await subject.start()
        await asyncio.sleep(0.3)
        await subject.stop(timeout=0.1)

        consumers.send(client, queue_url, "late")

        await sqs.wait_for(lambda: sqs.depth(client, queue_url) == (0, 1))
        assert client.receive_message(QueueUrl=queue_url).get("Messages") is None

    async def test_stop_before_start_does_nothing(self, queue_url: str):
        subject = consumers.build(queue_url)

        await subject.stop()

        assert not subject.is_running

    async def test_wait_returns_once_the_consumer_is_stopped(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        subject = consumers.build(queue_url, client=client)
        await subject.start()
        waiting = asyncio.create_task(subject.wait())

        await subject.stop()

        await asyncio.wait_for(waiting, timeout=5)
        assert waiting.done()


class TestHandlerRegistration:
    def test_the_decorator_registers_and_returns_the_function(self):
        subject: consumer.Consumer[object] = consumer.Consumer("q")

        @subject.handler
        async def handle(item: message.Message) -> None:
            return None

        assert subject._handler is handle

    def test_a_second_handler_is_rejected(self):
        subject = consumer.Consumer("q", consumers.accept)

        with pytest.raises(errors.ConfigurationError, match="already registered"):
            subject.handler(consumers.accept)

    def test_a_batch_consumer_registers_its_handler_the_same_way(self):
        subject: consumer.BatchConsumer[object] = consumer.BatchConsumer("q")

        @subject.handler
        async def handle(items) -> None:
            return None

        assert subject._batch_handler is handle

    async def test_starting_without_a_handler_is_rejected(self):
        subject: consumer.Consumer[object] = consumer.Consumer("q")

        with pytest.raises(errors.ConfigurationError, match="no handler registered"):
            await subject.start()

        assert not subject.is_running


class TestRun:
    async def test_cancelling_run_drains_in_flight_work_and_acks_it(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a")
        started = asyncio.Event()
        events = hook_events.Events()

        async def slow(item: message.Message) -> None:
            started.set()
            await asyncio.sleep(0.5)

        subject = consumers.build(
            queue_url, client=client, handler=slow, hooks=events.hooks()
        )
        running = asyncio.create_task(subject.run())
        await started.wait()

        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running

        assert events.bodies(events.processed) == ["a"]
        assert not subject.is_running
        assert sqs.depth(client, queue_url) == (0, 0)

    async def test_the_shutdown_timeout_cancels_handlers_and_deletes_nothing(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a")
        started = asyncio.Event()

        async def stuck(item: message.Message) -> None:
            started.set()
            await asyncio.sleep(30)

        subject = consumers.build(
            queue_url, client=client, handler=stuck, shutdown_timeout=0.2
        )
        running = asyncio.create_task(subject.run())
        await started.wait()

        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(running, timeout=5)

        assert sqs.depth(client, queue_url) == (0, 1)

    async def test_stop_makes_run_return(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        subject = consumers.build(queue_url, client=client)
        running = asyncio.create_task(subject.run())
        await sqs.wait_for(lambda: subject.is_running)

        await subject.stop()

        await asyncio.wait_for(running, timeout=5)
        assert running.done() and not running.cancelled()


class TestCrashLogging:
    async def test_a_crashed_consumer_task_is_logged_at_once(
        self,
        client: sqs_client.SQSClient,
        queue_url: str,
        caplog: pytest.LogCaptureFixture,
    ):
        subject = consumers.build(queue_url, client=client)

        async def crash(active: object) -> None:
            raise RuntimeError("loop bug")

        subject._poll = crash  # type: ignore[method-assign]
        with caplog.at_level(logging.ERROR, logger="sqs_consumer"):
            await subject.start()
            await sqs.wait_for(lambda: "stopped unexpectedly" in caplog.text)

        assert "loop bug" in caplog.text
        with pytest.raises(RuntimeError, match="loop bug"):
            await subject.stop()


def _stubbed_client() -> sqs_client.SQSClient:
    return boto3.client("sqs")


def _receive_response(*raws: dict[str, str]) -> dict[str, object]:
    return {"Messages": list(raws)}


class TestReportedFailures:
    """Faults Moto cannot produce, injected with botocore's Stubber."""

    async def test_partially_failed_deletes_are_reported_with_the_failed_entries(self):
        client = _stubbed_client()
        recorder = hook_events.Events()
        failed = {"Id": "0", "SenderFault": True, "Code": "ReceiptHandleIsInvalid"}
        with botocore.stub.Stubber(client) as stubber:
            stubber.add_response(
                "receive_message", _receive_response(sqs_messages.raw_message())
            )
            stubber.add_response(
                "delete_message_batch", {"Successful": [], "Failed": [failed]}
            )
            async with consumers.build(
                "http://queue", client=client, hooks=recorder.hooks()
            ):
                await sqs.wait_for(lambda: recorder.errors)

        error = recorder.errors[0]
        assert isinstance(error, errors.PartialFailureError)
        assert error.failed == [failed]

    async def test_a_release_that_fails_for_another_reason_is_reported_as_is(self):
        client = _stubbed_client()
        recorder = hook_events.Events()

        async def fail(item: message.Message) -> None:
            raise ValueError("handler failed")

        with botocore.stub.Stubber(client) as stubber:
            stubber.add_response(
                "receive_message", _receive_response(sqs_messages.raw_message())
            )
            stubber.add_client_error(
                "change_message_visibility_batch", service_error_code="InternalError"
            )
            async with consumers.build(
                "http://queue",
                client=client,
                handler=fail,
                hooks=recorder.hooks(),
                release_on_error=config.ReleaseOnError(),
            ):
                await sqs.wait_for(lambda: recorder.errors)

        error = recorder.errors[0]
        assert isinstance(error, botocore.exceptions.ClientError)
        assert error.response["Error"]["Code"] == "InternalError"

    async def test_a_delete_that_fails_for_another_reason_is_reported_as_is(self):
        client = _stubbed_client()
        recorder = hook_events.Events()
        with botocore.stub.Stubber(client) as stubber:
            stubber.add_response(
                "receive_message", _receive_response(sqs_messages.raw_message())
            )
            stubber.add_client_error(
                "delete_message_batch", service_error_code="InternalError"
            )
            async with consumers.build(
                "http://queue", client=client, hooks=recorder.hooks()
            ):
                await sqs.wait_for(lambda: recorder.errors)

        error = recorder.errors[0]
        assert isinstance(error, botocore.exceptions.ClientError)
        assert error.response["Error"]["Code"] == "InternalError"

    async def test_a_partially_failed_release_is_reported_with_the_failed_entries(
        self,
    ):
        client = _stubbed_client()
        recorder = hook_events.Events()
        failed = {"Id": "0", "SenderFault": True, "Code": "MessageNotInflight"}

        async def fail(item: message.Message) -> None:
            raise ValueError("handler failed")

        with botocore.stub.Stubber(client) as stubber:
            stubber.add_response(
                "receive_message", _receive_response(sqs_messages.raw_message())
            )
            stubber.add_response(
                "change_message_visibility_batch",
                {"Successful": [], "Failed": [failed]},
            )
            async with consumers.build(
                "http://queue",
                client=client,
                handler=fail,
                hooks=recorder.hooks(),
                release_on_error=config.ReleaseOnError(),
            ):
                await sqs.wait_for(lambda: recorder.errors)

        error = recorder.errors[0]
        assert isinstance(error, errors.PartialFailureError)
        assert error.action == "ChangeMessageVisibilityBatch"
        assert error.failed == [failed]

    async def test_a_malformed_receive_response_is_reported_and_polling_continues(
        self,
    ):
        client = _stubbed_client()
        recorder = hook_events.Events()
        with botocore.stub.Stubber(client) as stubber:
            stubber.add_response("receive_message", {"Messages": [{"Body": "x"}]})
            stubber.add_response(
                "receive_message", _receive_response(sqs_messages.raw_message())
            )
            stubber.add_response(
                "delete_message_batch", {"Successful": [{"Id": "0"}], "Failed": []}
            )
            async with consumers.build(
                "http://queue",
                client=client,
                hooks=recorder.hooks(),
                backoff=config.Backoff(initial=0.01, maximum=0.01),
            ):
                await sqs.wait_for(lambda: recorder.processed)

        assert isinstance(recorder.errors[0], KeyError)
        assert recorder.bodies(recorder.processed) == ["body-1"]


class TestPayloads:
    async def test_an_undecodable_payload_fails_the_message_and_keeps_it(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "not json")
        recorder = hook_events.Events()

        async def read_payload(item: message.Message) -> None:
            _ = item.payload

        async with consumers.build(
            queue_url, client=client, handler=read_payload, hooks=recorder.hooks()
        ):
            await sqs.wait_for(lambda: recorder.failures)

        [(error, _)] = recorder.failures
        assert isinstance(error, errors.DeserializationError)
        assert recorder.processed == []
        assert sqs.depth(client, queue_url) == (0, 1)

    async def test_a_plain_text_queue_is_fine_when_the_payload_is_never_read(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "plain text")
        recorder = hook_events.Events()

        async with consumers.build(queue_url, client=client, hooks=recorder.hooks()):
            await sqs.wait_for(lambda: recorder.processed)

        assert recorder.failures == []
        assert recorder.bodies(recorder.processed) == ["plain text"]


class TestBatchHandlerMisuse:
    async def test_a_returned_value_fails_the_batch_without_stopping(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        consumers.send(client, queue_url, "a")
        recorder = hook_events.Events()

        async def wrong(items):
            return []

        subject = consumers.build_batch(
            queue_url, wrong, client=client, hooks=recorder.hooks()
        )
        async with subject:
            await sqs.wait_for(lambda: recorder.failures)
            assert subject.is_running

        [(error, failed)] = recorder.failures
        assert isinstance(error, TypeError)
        assert "BatchFailureError" in str(error)
        assert recorder.bodies(failed) == ["a"]
        assert sqs.depth(client, queue_url) == (0, 1)


class TestFifoRequests:
    async def test_a_fifo_consumer_always_asks_sqs_for_the_message_group_id(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, fifo=True)
        requested = _record_attribute_names(client)

        async with consumers.build(
            queue_url,
            client=client,
            receive=_quick_poll(attribute_names=["SentTimestamp"]),
        ):
            await sqs.wait_for(lambda: requested)

        assert requested[0] == ["SentTimestamp", "MessageGroupId"]

    async def test_a_standard_queue_consumer_requests_only_what_it_was_given(
        self, client: sqs_client.SQSClient, queue_url: str
    ):
        requested = _record_attribute_names(client)

        async with consumers.build(
            queue_url,
            client=client,
            receive=_quick_poll(attribute_names=["SentTimestamp"]),
        ):
            await sqs.wait_for(lambda: requested)

        assert requested[0] == ["SentTimestamp"]

    async def test_a_fifo_consumer_asking_for_all_attributes_is_left_alone(
        self, client: sqs_client.SQSClient
    ):
        queue_url = sqs.create_queue(client, fifo=True)
        requested = _record_attribute_names(client)

        async with consumers.build(
            queue_url, client=client, receive=_quick_poll(attribute_names=["All"])
        ):
            await sqs.wait_for(lambda: requested)

        assert requested[0] == ["All"]


def _quick_poll(*, attribute_names: list[str]) -> config.Receive:
    return dataclasses.replace(consumers.QUICK_POLL, attribute_names=attribute_names)


def _record_attribute_names(client: sqs_client.SQSClient) -> list[list[str]]:
    requested: list[list[str]] = []
    client.meta.events.register(
        "before-call.sqs.ReceiveMessage",
        lambda params, **_: requested.append(
            json.loads(params["body"]).get("AttributeNames", [])
        ),
    )
    return requested
