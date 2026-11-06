"""SNS notification envelopes as SQS receives them without raw message delivery.

SHAPE from the AWS SNS docs (HTTP/S notification JSON, which SQS subscriptions
also use). The integration tests in tests/integration/test_sns_subscriptions.py
receive real envelopes from the emulator, so a drift here shows up there.
"""

import json
from typing import Any


def notification(message: str = '{"order_id": 42}', **overrides: Any) -> dict[str, Any]:
    envelope: dict[str, Any] = {
        "Type": "Notification",
        "MessageId": "sns-message-1",
        "TopicArn": "arn:aws:sns:us-east-1:123456789012:orders",
        "Subject": "Order created",
        "Message": message,
        "Timestamp": "2026-10-30T09:00:00.000Z",
        "SignatureVersion": "1",
        "Signature": "c2lnbmF0dXJl",
        "SigningCertURL": "https://sns.us-east-1.amazonaws.com/cert.pem",
        "UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe",
        "MessageAttributes": {"kind": {"Type": "String", "Value": "created"}},
    }
    envelope.update(overrides)
    return envelope


def notification_body(message: str = '{"order_id": 42}', **overrides: Any) -> str:
    return json.dumps(notification(message, **overrides))
