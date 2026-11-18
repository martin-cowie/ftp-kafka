# ftp-server

An in-memory FTP server built with [libunftp](https://github.com/bolcom/libunftp), which forwards
uploaded files to [Kafka](https://kafka.apache.org/) on request.

## Features

- FTP login is checked by Kafka: the FTP username and password are the user's Kafka credentials
- Greeting message sent to every new session: `This is a test FTP server`
- Each FTP session gets its own isolated in-memory storage, reset when the session ends
- Supports upload (`STOR`), download (`RETR`), delete (`DELE`), and rename (`RNFR`/`RNTO`)
- Files live in a single flat namespace — subdirectories (`MKD`/`RMD`) aren't supported
- Custom `SITE SEND` command publishes one or more uploaded files to a Kafka topic

## Configuration

Kafka settings are defined in `config.toml`, which is not committed to this repository
(it's listed in `.gitignore`). Copy the example to get started:

```sh
cp config.toml.example config.toml
```

```toml
[kafka]
brokers = "localhost:9092,otherhost:9092"
default_topic = "rust-topic"
message_timeout_ms = 5000
```

Edit `config.toml` to set your own Kafka settings. The `[kafka]` table is
required:

| Field | Required | Meaning |
|---|---|---|
| `brokers` | Yes | Comma-separated `host:port` list; the server needs only one to be reachable |
| `default_topic` | Yes | Topic a Session starts with; `SITE SEND` uses it until `SITE TOPIC` changes it |
| `message_timeout_ms` | No (default `5000`) | How long to wait for a message's delivery |
| `auth_timeout_ms` | No (default `5000`) | How long a login waits for Kafka to accept or reject the credentials |
| `security_protocol` | No (default `PLAINTEXT`) | `PLAINTEXT` or `SASL_PLAINTEXT` |
| `sasl_mechanism` | When `security_protocol` is `SASL_PLAINTEXT` | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512` |

An optional `[server]` table holds settings for the server itself:

| Field | Required | Meaning |
|---|---|---|
| `port` | No (default `2121`) | TCP port the FTP server listens on |
| `log_level` | No (default `info`) | Least severe messages logged: `trace`, `debug`, `info`, `warn` or `error` |

The server won't start if `brokers` or `default_topic` is missing, if `sasl_mechanism` is missing under
`SASL_PLAINTEXT`, or if `security_protocol` or `sasl_mechanism` has any other value.

## Authentication

The server keeps no user accounts. A login succeeds only if the Kafka cluster accepts the username and
password the FTP client supplies, and the session then publishes to Kafka as that user.

- **`PLAINTEXT`**: the cluster has no authentication, so any username and password is accepted as long
  as the broker is reachable. The server logs a warning at startup.
- **`SASL_PLAINTEXT`**: the credentials are sent to Kafka using the configured mechanism. Prefer
  `SCRAM-SHA-256` or `SCRAM-SHA-512`, which never send the password to Kafka; `PLAIN` does.

Traffic is not encrypted: FTP passwords cross the network in clear text until FTPS is added (#4), and
Kafka messages are unencrypted until Kafka TLS is. Every login costs a round trip to the broker, and no
logins succeed while the broker is unreachable.

## Running

```sh
cargo run
```

The server listens on `0.0.0.0:2121`, or on the `[server] port` from `config.toml`. Passive mode ports are `50000–65535`.

The server reads `config.toml` from the current directory. Use `--config <path>` (or `-c`) to read a different file.

## Usage

Connect with any FTP client. Example using `lftp`:

```sh
lftp -u alice,password123 ftp://127.0.0.1:2121
```

### Supported commands

| Command | Description |
|---|---|
| `STOR <file>` | Upload a file |
| `RETR <file>` | Download a file |
| `DELE <file>` | Delete a file |
| `RNFR`/`RNTO` | Rename a file |
| `SITE SEND [--topic <topic>] [--key <key>] [--header <name>=<value>]... <file>...` | Publish one or more uploaded files to Kafka |
| `SITE TOPICS` | List the topics on the Kafka cluster, one per line |
| `SITE TOPIC [<topic>]` | Report the Session topic, or set it |

`SITE SEND` publishes each named file as a separate Kafka message. `--key` defaults to each file's
name. The topic is, in order of precedence: `--topic`, then the Session topic, then
`config.toml`'s `[kafka] default_topic`.

`SITE TOPICS` lists the cluster's topics, queried with the Session's Kafka credentials; internal
topics (names starting `__`) are hidden. `SITE TOPIC` reports the Session topic, which starts as
`default_topic`. `SITE TOPIC <topic>` sets it for the rest of the Session, and is refused with a
`550` reply if the topic does not exist on the cluster.

`--header` adds a Kafka message header and may be repeated; every message in the command gets the
same headers, in the order given. The value is everything after the first `=`, so it may itself
contain `=`. Quote an argument that contains spaces:

```
SITE SEND --header content-type=application/json --header "source=ftp gateway" a.json b.json
```

### Example session

```
220 This is a test FTP server
USER alice
331 Password Required
PASS password123
230 User logged in, proceed
STOR hello.txt
226 File successfully written
SITE SEND hello.txt
200 Sent file "hello.txt" as message to rust-topic. ...
RNFR hello.txt
350 Tell me, what would you like the new name to be?
RNTO world.txt
250 Renamed
RETR world.txt
226 Transfer complete
DELE world.txt
250 Successfully removed
QUIT
221 bye
```

## Testing

```sh
cargo test
```

Runs the unit tests (in `src/`) and a black-box integration suite (`tests/ftp_integration.rs`) that
spawns the real `ftp-server` binary and drives it as an FTP client would, over TCP. It also runs
`tests/kafka_mock_integration.rs`, which checks that `SITE SEND` delivers payload, key and headers to
librdkafka's in-process mock Kafka cluster. None of these needs any external services.

A further end-to-end test, `tests/kafka_integration.rs`, exercises `SITE SEND` against a real Kafka
broker started via [testcontainers](https://testcontainers.com/). It requires a working Docker daemon,
so it's excluded from the default run:

```sh
cargo test --test kafka_integration -- --ignored
```

To check coverage (via [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov)):

```sh
cargo llvm-cov --show-missing-lines
```

## Implementation notes

`SITE SEND`, `SITE TOPICS` and `SITE TOPIC` are implemented with `SiteCommandHandler`s registered via
`ServerBuilder::site_command("send", KafkaSendHandler)` and its siblings, a generic `SITE` subcommand extension point
that was contributed upstream to [libunftp](https://github.com/bolcom/libunftp). 

Per-session storage isolation is achieved naturally: libunftp calls the storage factory (`MemStorage::new`) once per incoming TCP connection, so each session gets a fresh, empty `HashMap`.
