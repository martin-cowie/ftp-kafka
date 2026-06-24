# ftp-server

An in-memory FTP server built with [libunftp](https://github.com/bolcom/libunftp).

## Features

- Authentication configured via a TOML file
- Greeting message sent to every new session: `this is a test FTP server`
- Each FTP session gets its own isolated in-memory storage, reset when the session ends
- Supports upload (`STOR`), delete (`DELE`), and rename (`RNFR`/`RNTO`)
- Downloads (`RETR`) are blocked
- Custom `SITE TEST` command replies with `Hello world`

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

The server listens on `0.0.0.0:2121`. Passive mode ports are `50000–65535`.

## Usage

Connect with any FTP client. Example using `lftp`:

```sh
lftp -u alice,password123 ftp://127.0.0.1:2121
```

### Supported commands

| Command | Description |
|---|---|
| `STOR <file>` | Upload a file |
| `DELE <file>` | Delete a file |
| `RNFR`/`RNTO` | Rename a file |
| `RETR <file>` | Blocked — returns 550 Permission denied |
| `SITE TEST` | Replies with `200 Hello world` |

### Example session

```
220 this is a test FTP server
USER alice
331 Password Required
PASS password123
230 User logged in, proceed
SITE TEST
200 Hello world
STOR hello.txt
226 File successfully written
RNFR hello.txt
350 Tell me, what would you like the new name to be?
RNTO world.txt
250 Renamed
DELE world.txt
250 Successfully removed
QUIT
221 bye
```

## Implementation notes

`SITE TEST` is implemented with a `SiteCommandHandler` registered via `ServerBuilder::site_command("TEST", SiteTestHandler)`, a generic `SITE` subcommand extension point added to a fork of libunftp at `../libunftp` (referenced via a `path` dependency rather than a vendored copy).

Per-session storage isolation is achieved naturally: libunftp calls the storage factory (`|| MemStorage::new()`) once per incoming TCP connection, so each session gets a fresh, empty `HashMap`.
