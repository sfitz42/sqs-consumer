import json

from sqs_consumer import sns
from tests.fixtures import sns_envelopes


class TestParse:
    def test_returns_the_inner_message_and_metadata_of_a_notification(self):
        parsed = sns.parse(sns_envelopes.notification_body('{"order_id": 42}'))

        assert parsed is not None
        assert parsed.message == '{"order_id": 42}'
        assert parsed.topic_arn == "arn:aws:sns:us-east-1:123456789012:orders"
        assert parsed.subject == "Order created"
        assert parsed.message_attributes == {
            "kind": {"Type": "String", "Value": "created"}
        }

    def test_leaves_the_subject_empty_when_sns_omits_it(self):
        envelope = sns_envelopes.notification()
        del envelope["Subject"]

        parsed = sns.parse(json.dumps(envelope))

        assert parsed is not None
        assert parsed.subject is None

    def test_returns_none_for_a_body_that_is_not_json(self):
        assert sns.parse("plain text order 42") is None

    def test_returns_none_for_a_raw_delivery_payload(self):
        assert sns.parse('{"order_id": 42, "Message": "not an envelope"}') is None

    def test_returns_none_for_other_sns_message_types(self):
        body = sns_envelopes.notification_body(Type="SubscriptionConfirmation")

        assert sns.parse(body) is None

    def test_returns_none_when_the_inner_message_is_not_a_string(self):
        body = json.dumps(sns_envelopes.notification(Message={"order_id": 42}))

        assert sns.parse(body) is None
