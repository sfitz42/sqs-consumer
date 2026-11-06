import os
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from docker import errors as docker_errors
from mypy_boto3_sqs import client as sqs_client
from testcontainers.core import container, wait_strategies

from tests.support import sqs

FLOCI_IMAGE = (
    "floci/floci:2.3.0"
    "@sha256:e0b292df4dc9559b7b1bf4f51bdda4bb8e269276236438cd03cae661260ea8a9"
)
FLOCI_PORT = 4566


@pytest.fixture(scope="session")
def sqs_endpoint() -> Iterator[str]:
    """Floci in a container on a free port, or any endpoint named by the environment.

    Session scoped because every test creates its own uniquely named queue, so no
    state is shared. Set SQS_TEST_ENDPOINT_URL to run the same suite against
    another emulator or a real account.
    """
    external = os.environ.get("SQS_TEST_ENDPOINT_URL")
    if external:
        yield external
        return
    # Any HTTP response means Floci is serving; it answers "/" with an error code.
    floci = (
        container.DockerContainer(FLOCI_IMAGE)
        .with_exposed_ports(FLOCI_PORT)
        .waiting_for(
            wait_strategies.HttpWaitStrategy(FLOCI_PORT).for_status_code_matching(
                lambda _: True
            )
        )
    )
    try:
        floci.start()
    except docker_errors.DockerException as error:
        pytest.exit(
            f"could not start Floci ({error}); the integration tests need Docker, "
            "or set SQS_TEST_ENDPOINT_URL",
            returncode=1,
        )
    try:
        host = floci.get_container_host_ip()
        yield f"http://{host}:{floci.get_exposed_port(FLOCI_PORT)}"
    finally:
        floci.stop()


@pytest.fixture
def client(sqs_endpoint: str, aws_environment: None) -> sqs_client.SQSClient:
    return boto3.client("sqs", endpoint_url=sqs_endpoint)


@pytest.fixture
def queue_url(client: sqs_client.SQSClient) -> str:
    return sqs.create_queue(client)


@pytest.fixture
def sns_client(sqs_endpoint: str, aws_environment: None) -> Any:
    return boto3.client("sns", endpoint_url=sqs_endpoint)
