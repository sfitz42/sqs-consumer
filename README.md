# sqs-consumer

Build Amazon SQS-based applications without the boilerplate.

## Usage

```python
import asyncio

from sqs_consumer import consumer, message


async def handle(item: message.Message) -> None:
    print(item.payload)  # return to delete the message, raise to leave it on the queue


asyncio.run(
    consumer.Consumer("https://sqs.eu-west-2.amazonaws.com/123/my-queue", handle).run()
)
```

- Messages in a batch are handled concurrently. A message is deleted only if its
  handler returns.
- Handlers must be `async def`. Wrap blocking work in `asyncio.to_thread`.
- Errors are logged to the `sqs_consumer` logger and never stop the consumer.

## FastAPI

Start consumers in the lifespan, not in `BackgroundTasks`:

```python
@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with consumer.Consumer(settings.queue_url, handle, shutdown_timeout=10):
        yield


app = FastAPI(lifespan=lifespan)
```

Set `shutdown_timeout` below uvicorn's `--timeout-graceful-shutdown`. Without it,
shutdown waits for in-flight handlers indefinitely.

## Credentials and IAM

The consumer creates a boto3 client, so it uses the standard credential chain and
`AWS_REGION`/`AWS_DEFAULT_REGION`. Pass `client=` to use your own.

Required: `sqs:ReceiveMessage`, `sqs:DeleteMessage`. Also needed:

| Option | Extra action |
|---|---|
| `heartbeat`, `release_on_error`, or raising `RetryAfterError` | `sqs:ChangeMessageVisibility` |
| `check_queue=True` | `sqs:GetQueueAttributes` |
| Queue encrypted with a customer-managed KMS key | `kms:Decrypt` |

`consumer.required_iam_actions()` returns the SQS actions for your configuration.

## FIFO queues

Queue URLs ending in `.fifo` are treated as FIFO. Groups run concurrently, but
messages within a group run one at a time, in order. If a message fails, the rest of
its group in that batch is not handled or deleted, so they're retried in order.
They're reported to `on_processing_error` along with the failed message.

`BatchConsumer` gets the batch in SQS order and does not enforce group ordering.

## Caveats

- Stopping can take up to `wait_time_seconds` (default 20), because a long poll in
  progress can't be cancelled.
- If the shutdown timeout is reached, handlers are cancelled and none of the batch is
  deleted.
- Each consumer handles one batch at a time. Run more consumers for more throughput.

## API

### `Consumer(queue_url, handler=None, **options)`

Calls `handler(message)` once per message. You can also register the handler with
the `@consumer.handler` decorator.

### `BatchConsumer(queue_url, batch_handler=None, **options)`

Calls `batch_handler(messages)` once per batch. Returning deletes every message in
the batch. To fail some messages, raise `BatchFailureError({message: exc, ...})`.
Any other exception fails the whole batch.

### Methods

| Method | Description |
|---|---|
| `await run()` | Consume until cancelled or `stop()` is called |
| `async with consumer:` | Consume in the background while inside the block |
| `await start()` / `await stop(timeout=None)` | Start or stop manually |
| `is_running` | Whether the consumer is running |
| `required_iam_actions()` | IAM actions this configuration needs |

### Options

| Option | Default | Description |
|---|---|---|
| `deserializer` | `json.loads` | Parses `message.payload` from the body, e.g. `Model.model_validate_json` |
| `receive` | `Receive()` | Poll settings: `wait_time_seconds=20`, `max_messages=10`, `visibility_timeout`, `attribute_names`, `message_attribute_names` |
| `handler_timeout` | `None` | Seconds before a handler call times out |
| `shutdown_timeout` | `None` | Seconds to wait for in-flight handlers on stop |
| `delete_on_success` | `True` | Delete messages after they're handled |
| `heartbeat` | `None` | `Heartbeat(interval, visibility_timeout)` extends visibility while handlers run |
| `release_on_error` | `None` | `ReleaseOnError()` makes failed messages visible again immediately |
| `check_queue` | `False` | On start, check the queue exists and its visibility timeout is longer than the heartbeat interval |
| `unwrap_sns` | `False` | Parse the payload from inside an SNS envelope (`message.sns` has its metadata) |
| `backoff` | `Backoff()` | Delay between retries after a failed receive |
| `hooks` | `Hooks()` | Async callbacks: `on_message_received`, `on_message_processed`, `on_processing_error`, `on_empty`, `on_stopped` |
| `logger` | `sqs_consumer` logger | A `Logger` or `LoggerAdapter` to log to |
| `client` | new boto3 client | SQS client to use |
| `io_threads` | `4` | Threads for blocking boto3 calls |

### Errors

| Error | Description |
|---|---|
| `RetryAfterError(delay)` | Raise in a handler to retry the message after `delay` seconds |
| `BatchFailureError(failures)` | Raise in a batch handler to fail only some messages |
| `DeserializationError` | Raised when reading `message.payload` fails |
| `ConfigurationError` | Invalid options, or no handler registered |
| `MissingPermissionError` | Logged when an IAM action is denied |

## Development

```sh
uv sync
uv run task test
```

`task test` runs `lint`, `typecheck` and then pytest; each is also a task of its
own, and `uv run task format` fixes formatting and lint in place.

The integration tests start [Floci](https://floci.io/aws/) in a container with Testcontainers, so they
need Docker. To use an already running emulator or a real account instead, set
`SQS_TEST_ENDPOINT_URL`.
