"""The message type handed to user handlers."""

import dataclasses
import json
from collections.abc import Callable, Mapping
from typing import Any, overload

from sqs_consumer import errors
from sqs_consumer import sns as sns_envelope


@dataclasses.dataclass(frozen=True, slots=True, eq=False)
class Message[T]:
    """One delivery of an SQS message. Equal and hashed by receipt handle.

    Attributes:
        body: The SQS body exactly as received, SNS envelope and all.
        message_id: The SQS message ID.
        receipt_handle: The handle for this delivery, used to delete or release it.
        attributes: The system attributes requested by Receive.attribute_names.
        message_attributes: Message attributes in SQS's typed shape; see
            message_attribute_values for plain values.
        raw: The ReceiveMessage entry this was built from.
        sns: The SNS envelope, when the consumer unwraps SNS and the body was one.
        deserializer: Turns the payload source into `payload`.
    """

    body: str
    message_id: str
    receipt_handle: str
    attributes: Mapping[str, str]
    message_attributes: Mapping[str, Any]
    raw: Mapping[str, Any]
    sns: sns_envelope.SnsNotification | None = None
    deserializer: Callable[[str], T] = dataclasses.field(
        default=json.loads, repr=False, compare=False
    )
    _cache: dict[str, T] = dataclasses.field(
        default_factory=dict, repr=False, compare=False
    )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Message):
            return NotImplemented

        return self.receipt_handle == other.receipt_handle

    def __hash__(self) -> int:
        return hash(self.receipt_handle)

    @property
    def message_group_id(self) -> str | None:
        """The FIFO message group, or None for a standard queue."""
        return self.attributes.get("MessageGroupId")

    @property
    def trace_header(self) -> str | None:
        """The X-Ray trace header; needs "AWSTraceHeader" in Receive.attribute_names."""
        return self.attributes.get("AWSTraceHeader")

    @property
    def message_attribute_values(self) -> dict[str, str | int | float | bytes]:
        """Message attributes as plain values instead of SQS's typed shape.

        String gives str, Binary gives bytes and Number gives int, or float if it
        is not a whole number. Custom types such as Number.price use the base type.
        """
        return {
            name: _attribute_value(attribute)
            for name, attribute in self.message_attributes.items()
        }

    @property
    def payload(self) -> T:
        """The SNS inner message, or else the body, run through the deserializer.

        Parsed on first access and cached.

        Raises:
            errors.DeserializationError: The deserializer raised. It fails the
                message like any other handler error.
        """
        if "payload" not in self._cache:
            source = self.sns.message if self.sns is not None else self.body

            try:
                self._cache["payload"] = self.deserializer(source)
            except Exception as exc:
                raise errors.DeserializationError(self.message_id) from exc

        return self._cache["payload"]

    @overload
    @classmethod
    def from_sqs(
        cls, raw: Mapping[str, Any], *, unwrap_sns: bool = False
    ) -> "Message[Any]": ...

    @overload
    @classmethod
    def from_sqs[U](
        cls,
        raw: Mapping[str, Any],
        *,
        deserializer: Callable[[str], U],
        unwrap_sns: bool = False,
    ) -> "Message[U]": ...

    @classmethod
    def from_sqs(
        cls,
        raw: Mapping[str, Any],
        *,
        deserializer: Callable[[str], Any] = json.loads,
        unwrap_sns: bool = False,
    ) -> "Message[Any]":
        """Build a Message from one ReceiveMessage entry.

        Args:
            raw: The entry, as returned in the response's Messages list.
            deserializer: Turns the payload source into `payload`.
            unwrap_sns: Parse an SNS envelope in the body into `sns`.

        Returns:
            The message.
        """
        body = raw.get("Body", "")

        return Message(
            body=body,
            message_id=raw["MessageId"],
            receipt_handle=raw["ReceiptHandle"],
            attributes=raw.get("Attributes", {}),
            message_attributes=raw.get("MessageAttributes", {}),
            raw=raw,
            sns=sns_envelope.parse(body) if unwrap_sns else None,
            deserializer=deserializer,
        )


def _attribute_value(attribute: Mapping[str, Any]) -> str | int | float | bytes:
    data_type = str(attribute.get("DataType", "String")).split(".", 1)[0]
    if data_type == "Binary":
        return bytes(attribute["BinaryValue"])

    value = str(attribute["StringValue"])

    if data_type == "Number":
        try:
            return int(value)
        except ValueError:
            return float(value)

    return value
