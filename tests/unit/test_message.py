import json

import pytest

from sqs_consumer import errors, message
from tests.fixtures import sns_envelopes, sqs_messages


class TestFromSqs:
    def test_defaults_the_optional_fields_when_sqs_omits_them(self):
        raw = {"MessageId": "m-1", "ReceiptHandle": "r-1"}

        received = message.Message.from_sqs(raw)

        assert received.body == ""
        assert received.attributes == {}
        assert received.message_attributes == {}

    def test_keeps_the_raw_response_entry(self):
        raw = sqs_messages.raw_message(7, Attributes={"SentTimestamp": "1"})

        received = message.Message.from_sqs(raw)

        assert received.raw is raw
        assert received.attributes == {"SentTimestamp": "1"}
        assert received.receipt_handle == "receipt-7"


class TestMessageGroupId:
    def test_is_none_for_a_standard_queue_message(self):
        received = message.Message.from_sqs(sqs_messages.raw_message())

        assert received.message_group_id is None

    def test_comes_from_the_fifo_system_attribute(self):
        raw = sqs_messages.raw_message(Attributes={"MessageGroupId": "orders-42"})

        assert message.Message.from_sqs(raw).message_group_id == "orders-42"


class TestPayload:
    def test_deserializes_the_body_as_json_by_default(self):
        raw = sqs_messages.raw_message(Body='{"order_id": 42}')

        assert message.Message.from_sqs(raw).payload == {"order_id": 42}

    def test_deserializes_the_inner_sns_message_when_unwrapping(self):
        raw = sqs_messages.raw_message(
            Body=sns_envelopes.notification_body('{"order_id": 42}')
        )

        received = message.Message.from_sqs(raw, unwrap_sns=True)

        assert received.payload == {"order_id": 42}
        assert received.sns is not None
        assert received.body == raw["Body"]

    def test_treats_the_whole_envelope_as_the_payload_when_not_unwrapping(self):
        raw = sqs_messages.raw_message(Body=sns_envelopes.notification_body())

        received = message.Message.from_sqs(raw)

        assert received.sns is None
        assert received.payload["Type"] == "Notification"

    def test_uses_the_body_when_unwrapping_finds_no_envelope(self):
        raw = sqs_messages.raw_message(Body='{"order_id": 42}')

        received = message.Message.from_sqs(raw, unwrap_sns=True)

        assert received.sns is None
        assert received.payload == {"order_id": 42}

    def test_uses_a_custom_deserializer(self):
        raw = sqs_messages.raw_message(Body="order:42")

        received = message.Message.from_sqs(
            raw, deserializer=lambda text: int(text.split(":")[1])
        )

        assert received.payload == 42

    def test_deserializes_only_once(self):
        calls: list[str] = []

        def counting(text: str) -> object:
            calls.append(text)
            return json.loads(text)

        received = message.Message.from_sqs(
            sqs_messages.raw_message(Body="[1]"), deserializer=counting
        )

        assert received.payload == received.payload == [1]
        assert calls == ["[1]"]

    def test_raises_a_deserialization_error_naming_the_message(self):
        received = message.Message.from_sqs(
            sqs_messages.raw_message(3, Body="not json")
        )

        with pytest.raises(errors.DeserializationError, match="message-3") as raised:
            _ = received.payload

        assert isinstance(raised.value.__cause__, json.JSONDecodeError)


class TestIdentity:
    def test_deliveries_with_the_same_receipt_handle_are_equal(self):
        first = message.Message.from_sqs(sqs_messages.raw_message(1))
        again = message.Message.from_sqs(
            sqs_messages.raw_message(1, Attributes={"SentTimestamp": "1"})
        )

        assert first == again
        assert hash(first) == hash(again)
        assert first != message.Message.from_sqs(sqs_messages.raw_message(2))

    def test_can_be_used_as_a_mapping_key(self):
        received = message.Message.from_sqs(
            sqs_messages.raw_message(Attributes={"SentTimestamp": "1"})
        )

        assert {received: "failed"}[received] == "failed"


class TestTraceHeader:
    def test_comes_from_the_aws_trace_header_attribute(self):
        raw = sqs_messages.raw_message(Attributes={"AWSTraceHeader": "Root=1-abc"})

        assert message.Message.from_sqs(raw).trace_header == "Root=1-abc"

    def test_is_none_when_not_requested(self):
        assert message.Message.from_sqs(sqs_messages.raw_message()).trace_header is None


class TestMessageAttributeValues:
    def test_decodes_each_data_type_to_a_plain_value(self):
        raw = sqs_messages.raw_message(
            MessageAttributes={
                "kind": {"DataType": "String", "StringValue": "created"},
                "count": {"DataType": "Number", "StringValue": "3"},
                "price": {"DataType": "Number.gbp", "StringValue": "9.99"},
                "blob": {"DataType": "Binary", "BinaryValue": b"\x00\x01"},
            }
        )

        values = message.Message.from_sqs(raw).message_attribute_values

        assert values == {
            "kind": "created",
            "count": 3,
            "price": 9.99,
            "blob": b"\x00\x01",
        }
