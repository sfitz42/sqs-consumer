from collections.abc import Iterator

import boto3
import moto
import pytest
from mypy_boto3_sqs import client as sqs_client

from tests.support import iam, sqs


@pytest.fixture
def client(aws_environment: None) -> Iterator[sqs_client.SQSClient]:
    with moto.mock_aws():
        yield boto3.client("sqs")


@pytest.fixture
def queue_url(client: sqs_client.SQSClient) -> str:
    return sqs.create_queue(client)


@pytest.fixture
def iam_enforcement() -> Iterator[iam.IamEnforcement]:
    enforcement = iam.IamEnforcement()
    yield enforcement
    enforcement.disable()
