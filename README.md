# ftp-server

An in-memory FTP server built with [libunftp](https://github.com/bolcom/libunftp), which forwards
uploaded files to [Kafka](https://kafka.apache.org/) on request.

## Features

- Authentication configured via a TOML file
- Greeting message sent to every new session: `This is a test FTP server`
- Each FTP session gets its own isolated in-memory storage, reset when the session ends
- Supports upload (`STOR`), download (`RETR`), delete (`DELE`), and rename (`RNFR`/`RNTO`)
- Files live in a single flat namespace — subdirectories (`MKD`/`RMD`) aren't supported
- Custom `SITE SEND` command publishes one or more uploaded files to a Kafka topic

## Configuration

Users are defined in `config.toml`:

```toml
[[users]]
username = "alice"
password = "password123"

[[users]]
username = "bob"
password = "secret456"
```

## Running

```sh
cargo run
```

The server listens on `0.0.0.0:2121`. Passive mode ports are `50000–65535`. Kafka messages are sent
to a broker at `localhost:9092`.

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
| `SITE SEND [--topic <topic>] [--key <key>] <file>...` | Publish one or more uploaded files to Kafka |

`SITE SEND` publishes each named file as a separate Kafka message. `--topic` defaults to
`rust-topic`; `--key` defaults to each file's name.

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

## Implementation notes

`SITE SEND` is implemented with a `SiteCommandHandler` registered via
`ServerBuilder::site_command("send", KafkaSendHandler)`, a generic `SITE` subcommand extension point
that was contributed upstream to [libunftp](https://github.com/bolcom/libunftp). It hasn't shipped in
a tagged `libunftp` release yet, so this project depends on `libunftp`'s `master` branch directly
rather than a published crates.io version.

Per-session storage isolation is achieved naturally: libunftp calls the storage factory (`|| MemStorage::new()`) once per incoming TCP connection, so each session gets a fresh, empty `HashMap`.
