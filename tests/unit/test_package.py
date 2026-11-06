import importlib.metadata


def test_package_is_installed() -> None:
    assert importlib.metadata.version("sqs-consumer") == "0.2.0"


def test_the_public_api_is_importable_from_the_package() -> None:
    import sqs_consumer
    from sqs_consumer import consumer, message

    assert sqs_consumer.Consumer is consumer.Consumer
    assert sqs_consumer.Message is message.Message
    assert all(hasattr(sqs_consumer, name) for name in sqs_consumer.__all__)
