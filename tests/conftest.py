from collections.abc import Iterator

import pytest

from tests.support import events


@pytest.fixture(autouse=True)
def aws_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fake credentials so no test can reach a real AWS account or profile."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)


@pytest.fixture(autouse=True)
def detach_error_collectors() -> Iterator[None]:
    """Events attaches a logging handler per instance; drop them after each test."""
    yield
    events.detach_collectors()
