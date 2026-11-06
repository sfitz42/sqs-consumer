"""Subscribe a queue to an SNS topic on the emulator."""

import uuid
from typing import Any

from mypy_boto3_sqs import client as sqs_client


def subscribe_new_topic(
    sns_client: Any,
    client: sqs_client.SQSClient,
    queue_url: str,
    *,
    raw_delivery: bool,
    fifo: bool = False,
) -> str:
    """Create a topic, subscribe the queue to it, and return the topic ARN."""
    name = f"topic-{uuid.uuid4().hex[:12]}"
    attributes: dict[str, str] = {}
    if fifo:
        name += ".fifo"
        attributes = {"FifoTopic": "true", "ContentBasedDeduplication": "true"}
    topic_arn: str = sns_client.create_topic(Name=name, Attributes=attributes)[
        "TopicArn"
    ]
    queue_arn = client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        Attributes={"RawMessageDelivery": "true" if raw_delivery else "false"},
    )
    return topic_arn
