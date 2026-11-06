"""Moto's IAM-like access control, so tests get a real AccessDenied from SQS."""

import json
from collections.abc import Sequence

import boto3
import moto.settings
from moto.core import authorization
from mypy_boto3_sqs import client as sqs_client

# Real AWS authorises the batch APIs under the single-message action (the IAM
# reference lists no sqs:DeleteMessageBatch), but Moto checks the API name, so the
# policy has to name both for Moto to behave like AWS.
BATCH_FORMS = {
    "sqs:DeleteMessage": "sqs:DeleteMessageBatch",
    "sqs:ChangeMessageVisibility": "sqs:ChangeMessageVisibilityBatch",
}


def create_limited_client(allowed_actions: Sequence[str]) -> sqs_client.SQSClient:
    """An SQS client whose user may only perform the given actions.

    Nothing is enforced until IamEnforcement.enable() is called, so queues can
    be seeded with the ordinary test credentials first.
    """
    actions = [
        *allowed_actions,
        *(BATCH_FORMS[a] for a in allowed_actions if a in BATCH_FORMS),
    ]
    iam = boto3.client("iam")
    iam.create_user(UserName="limited")
    policy = iam.create_policy(
        PolicyName="limited",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": actions,
                        "Resource": "*",
                    }
                ],
            }
        ),
    )["Policy"]["Arn"]
    iam.attach_user_policy(UserName="limited", PolicyArn=policy)
    key = iam.create_access_key(UserName="limited")["AccessKey"]
    return boto3.client(
        "sqs",
        aws_access_key_id=key["AccessKeyId"],
        aws_secret_access_key=key["SecretAccessKey"],
    )


class IamEnforcement:
    def __init__(self) -> None:
        self._original = moto.settings.INITIAL_NO_AUTH_ACTION_COUNT

    def enable(self) -> None:
        moto.settings.INITIAL_NO_AUTH_ACTION_COUNT = 0
        authorization.ActionAuthenticatorMixin.request_count = 0

    def disable(self) -> None:
        moto.settings.INITIAL_NO_AUTH_ACTION_COUNT = self._original
