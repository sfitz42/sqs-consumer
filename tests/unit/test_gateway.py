import concurrent.futures
import contextvars
import json
import threading
from collections.abc import Iterator

import boto3
import botocore.exceptions
import botocore.stub
import pytest
from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import config, errors, gateway, message
from tests.support import sqs

trace_id: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id")


@pytest.fixture
def pool() -> Iterator[concurrent.futures.ThreadPoolExecutor]:
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=2, thread_name_prefix="sqs-consumer"
    )
    yield executor
    executor.shutdown()


async def receive_all(queue: gateway.Gateway, expected: int) -> list[message.Message]:
    received: list[message.Message] = []
    while len(received) < expected:
        raws = await queue.receive(
            config.Receive(wait_time_seconds=0), orphan_release_timeout=None
        )
        received += [message.Message.from_sqs(raw) for raw in raws]
    return received


class TestDelete:
    async def test_sends_delete_batches_of_at_most_ten_entries(
        self, client: sqs_client.SQSClient, queue_url: str, pool
    ):
        for number in range(25):
            client.send_message(QueueUrl=queue_url, MessageBody=str(number))
        batch_sizes: list[int] = []
        client.meta.events.register(
            "before-call.sqs.DeleteMessageBatch",
            lambda params, **_: batch_sizes.append(
                len(json.loads(params["body"])["Entries"])
            ),
        )
        queue = gateway.Gateway(client, queue_url, pool)
        received = await receive_all(queue, 25)

        await queue.delete(received)

        assert batch_sizes == [10, 10, 5]
        assert sqs.depth(client, queue_url) == (0, 0)

    async def test_raises_with_the_failed_entries_after_trying_every_chunk(self, pool):
        client = boto3.client("sqs")
        queue = gateway.Gateway(client, "http://queue", pool)
        messages = [message.Message.from_sqs(_raw(n)) for n in range(11)]
        failed = {"Id": "0", "SenderFault": True, "Code": "Bad", "Message": "no"}
        with botocore.stub.Stubber(client) as stubber:
            stubber.add_response(
                "delete_message_batch", {"Successful": [], "Failed": [failed]}
            )
            stubber.add_response(
                "delete_message_batch", {"Successful": [], "Failed": []}
            )

            with pytest.raises(errors.PartialFailureError, match="1 entries") as raised:
                await queue.delete(messages)

            stubber.assert_no_pending_responses()
        assert raised.value.failed == [failed]


class TestChangeVisibilityEach:
    async def test_gives_each_message_its_own_timeout_in_batches_of_ten(self, pool):
        client = boto3.client("sqs")
        queue = gateway.Gateway(client, "http://queue", pool)
        entries = [(message.Message.from_sqs(_raw(n)), n) for n in range(11)]
        with botocore.stub.Stubber(client) as stubber:
            for chunk in (entries[:10], entries[10:]):
                stubber.add_response(
                    "change_message_visibility_batch",
                    {"Successful": [], "Failed": []},
                    {
                        "QueueUrl": "http://queue",
                        "Entries": [
                            {
                                "Id": str(i),
                                "ReceiptHandle": m.receipt_handle,
                                "VisibilityTimeout": timeout,
                            }
                            for i, (m, timeout) in enumerate(chunk)
                        ],
                    },
                )

            await queue.change_visibility_each(entries)

            stubber.assert_no_pending_responses()


class TestThreading:
    async def test_runs_calls_on_the_consumers_pool_with_the_callers_context(
        self, client: sqs_client.SQSClient, queue_url: str, pool
    ):
        seen: list[tuple[str, str]] = []
        client.meta.events.register(
            "before-call.sqs.ReceiveMessage",
            lambda **_: seen.append((threading.current_thread().name, trace_id.get())),
        )
        queue = gateway.Gateway(client, queue_url, pool)
        trace_id.set("trace-123")

        await receive_all_once(queue)

        thread_name, seen_trace = seen[0]
        assert thread_name.startswith("sqs-consumer")
        assert seen_trace == "trace-123"


async def receive_all_once(queue: gateway.Gateway) -> None:
    await queue.receive(
        config.Receive(max_messages=1, wait_time_seconds=0), orphan_release_timeout=None
    )


def _raw(number: int) -> dict[str, str]:
    return {"MessageId": f"m-{number}", "ReceiptHandle": f"r-{number}"}


def _client_error(code: str, message_text: str = "") -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": message_text}}, "ReceiveMessage"
    )


class TestIsAccessDenied:
    @pytest.mark.parametrize("code", ["AccessDenied", "AccessDeniedException"])
    def test_recognises_the_access_denied_codes(self, code):
        assert gateway.is_access_denied(_client_error(code))

    def test_recognises_moto_reporting_it_as_a_403_with_the_xml_in_the_message(self):
        error = _client_error("403", "<Code>AccessDenied</Code>")

        assert gateway.is_access_denied(error)

    def test_does_not_treat_a_missing_queue_as_access_denied(self):
        error = _client_error("AWS.SimpleQueueService.NonExistentQueue", "gone")

        assert not gateway.is_access_denied(error)

    def test_ignores_exceptions_that_are_not_client_errors(self):
        assert not gateway.is_access_denied(RuntimeError("AccessDenied"))
