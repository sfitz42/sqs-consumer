import asyncio
import concurrent.futures
from collections.abc import Iterator

import boto3
import botocore.exceptions
import botocore.stub
import pytest
from mypy_boto3_sqs import client as sqs_client

from sqs_consumer import config, gateway, heartbeat, message
from tests.support import iam


@pytest.fixture
def pool() -> Iterator[concurrent.futures.ThreadPoolExecutor]:
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    yield executor
    executor.shutdown()


class Reports:
    def __init__(self) -> None:
        self.errors: list[Exception] = []
        self.denied: list[Exception] = []

    async def report(self, exc: Exception) -> None:
        self.errors.append(exc)

    async def deny(self, exc: Exception) -> None:
        self.denied.append(exc)


async def receive_one(
    queue: gateway.Gateway, visibility_timeout: int
) -> list[message.Message]:
    raws = await queue.receive(
        config.Receive(wait_time_seconds=0, visibility_timeout=visibility_timeout),
        orphan_release_timeout=None,
    )
    return [message.Message.from_sqs(raw) for raw in raws]


class TestRun:
    async def test_keeps_a_message_invisible_beyond_its_original_timeout(
        self, client: sqs_client.SQSClient, queue_url: str, pool
    ):
        client.send_message(QueueUrl=queue_url, MessageBody="slow")
        queue = gateway.Gateway(client, queue_url, pool)
        received = await receive_one(queue, visibility_timeout=1)
        reports = Reports()
        beat = asyncio.create_task(
            heartbeat.run(
                queue,
                received,
                config.Heartbeat(interval=0.3, visibility_timeout=5),
                report=reports.report,
                denied=reports.deny,
            )
        )

        await asyncio.sleep(1.8)
        beat.cancel()

        assert client.receive_message(QueueUrl=queue_url).get("Messages") is None
        assert reports.errors == []

    async def test_reports_a_transient_failure_and_extends_on_the_next_tick(self, pool):
        client = boto3.client("sqs")
        queue = gateway.Gateway(client, "http://queue", pool)
        received = [message.Message.from_sqs({"MessageId": "1", "ReceiptHandle": "r"})]
        reports = Reports()
        with botocore.stub.Stubber(client) as stubber:
            stubber.add_client_error(
                "change_message_visibility_batch", service_error_code="Throttling"
            )
            stubber.add_response(
                "change_message_visibility_batch",
                {"Successful": [{"Id": "0"}], "Failed": []},
            )
            beat = asyncio.create_task(
                heartbeat.run(
                    queue,
                    received,
                    config.Heartbeat(interval=0.05, visibility_timeout=5),
                    report=reports.report,
                    denied=reports.deny,
                )
            )

            await asyncio.sleep(0.4)
            beat.cancel()

            stubber.assert_no_pending_responses()

        first = reports.errors[0]
        assert isinstance(first, botocore.exceptions.ClientError)
        assert first.response["Error"]["Code"] == "Throttling"
        assert reports.denied == []

    async def test_stops_and_hands_over_the_error_when_access_is_denied(
        self,
        client: sqs_client.SQSClient,
        queue_url: str,
        iam_enforcement: iam.IamEnforcement,
        pool,
    ):
        client.send_message(QueueUrl=queue_url, MessageBody="x")
        limited = iam.create_limited_client(["sqs:ReceiveMessage"])
        queue = gateway.Gateway(limited, queue_url, pool)
        iam_enforcement.enable()
        received = await receive_one(queue, visibility_timeout=30)
        reports = Reports()

        await asyncio.wait_for(
            heartbeat.run(
                queue,
                received,
                config.Heartbeat(interval=0.05, visibility_timeout=5),
                report=reports.report,
                denied=reports.deny,
            ),
            timeout=5,
        )

        assert len(reports.denied) == 1
        assert gateway.is_access_denied(reports.denied[0])
        assert reports.errors == []
