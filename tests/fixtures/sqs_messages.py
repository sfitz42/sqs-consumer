"""ReceiveMessage response entries.

SHAPE taken from the boto3 SQS service model (Message structure); the real
responses Floci returns are exercised by the integration tests.
"""

from typing import Any


def raw_message(number: int = 1, **overrides: Any) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "MessageId": f"message-{number}",
        "ReceiptHandle": f"receipt-{number}",
        "Body": f"body-{number}",
    }

    raw.update(overrides)

    return raw
