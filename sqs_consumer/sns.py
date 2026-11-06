"""SNS notification envelopes, as delivered to SQS without raw message delivery."""

import dataclasses
import json
from collections.abc import Mapping
from typing import Any


@dataclasses.dataclass(frozen=True, slots=True)
class SnsNotification:
    """An SNS notification envelope.

    Attributes:
        message_id: The SNS message ID.
        topic_arn: The topic the notification was published to.
        message: The published message.
        timestamp: When it was published, as SNS formats it.
        subject: The subject, if one was published.
        message_attributes: SNS shape, e.g.
            {"kind": {"Type": "String", "Value": "created"}}.
        raw: The decoded envelope.
    """

    message_id: str
    topic_arn: str
    message: str
    timestamp: str
    subject: str | None
    message_attributes: Mapping[str, Any]
    raw: Mapping[str, Any]


def parse(body: str) -> SnsNotification | None:
    """Parse the SNS envelope in an SQS body.

    A body counts as an envelope only if it is a JSON object with
    Type "Notification" and string TopicArn and Message fields.

    Args:
        body: The SQS message body.

    Returns:
        The envelope, or None if the body is not one, including the payload
        itself when raw message delivery is on.
    """
    try:
        envelope = json.loads(body)
    except ValueError:
        return None

    if (
        not isinstance(envelope, dict)
        or envelope.get("Type") != "Notification"
        or not isinstance(envelope.get("TopicArn"), str)
        or not isinstance(envelope.get("Message"), str)
    ):
        return None

    return SnsNotification(
        message_id=str(envelope.get("MessageId", "")),
        topic_arn=envelope["TopicArn"],
        message=envelope["Message"],
        timestamp=str(envelope.get("Timestamp", "")),
        subject=envelope.get("Subject"),
        message_attributes=envelope.get("MessageAttributes", {}),
        raw=envelope,
    )
