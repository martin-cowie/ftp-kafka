//! Shared black-box test harness: spawns the real `ftp-server` binary in a
//! temp directory (so it picks up a throwaway `config.toml`) and drives it
//! as a plain FTP client would, over real TCP.
//!
//! Not every item here is used by every test binary that includes this
//! module, since each one only exercises the parts it needs.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub const USERNAME: &str = "alice";
pub const PASSWORD: &str = "password123";

pub struct TestServer {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl TestServer {
    /// Spawns the server with the default `config.toml` (alice/bob), on a free port.
    pub fn start() -> Self {
        Self::start_with_config(&format!(
            r#"
                [[users]]
                username = "{USERNAME}"
                password = "{PASSWORD}"
            "#
        ))
    }

    pub fn start_with_config(config_toml: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("config.toml"), config_toml).unwrap();

        let port = free_port();
        let child = Command::new(env!("CARGO_BIN_EXE_ftp-server"))
            .current_dir(dir.path())
            .arg("--port")
            .arg(port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn ftp-server binary");

        let server = TestServer { child, port, _dir: dir };
        server.wait_until_accepting_connections();
        server
    }

    /// Spawns a second server in the same directory, reusing the same port -
    /// used to exercise the "address already in use" startup-failure path.
    pub fn start_conflicting(existing: &TestServer) -> Child {
        Command::new(env!("CARGO_BIN_EXE_ftp-server"))
            .current_dir(existing._dir.path())
            .arg("--port")
            .arg(existing.port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn second ftp-server binary")
    }

    fn wait_until_accepting_connections(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            if Instant::now() > deadline {
                panic!("ftp-server did not start listening on port {} in time", self.port);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn connect(&self) -> FtpClient {
        FtpClient::connect(self.port)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// A minimal, blocking FTP client, just enough to drive the commands this
/// server implements.
pub struct FtpClient {
    reader: BufReader<TcpStream>,
}

impl FtpClient {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let mut client = FtpClient { reader: BufReader::new(stream) };
        let greeting = client.read_response();
        assert!(greeting.starts_with("220"), "unexpected greeting: {greeting}");
        client
    }

    pub fn login(&mut self, username: &str, password: &str) -> String {
        self.command(&format!("USER {username}"));
        self.command(&format!("PASS {password}"))
    }

    /// Sends one command line and returns the (possibly multi-line) response.
    pub fn command(&mut self, line: &str) -> String {
        let stream = self.reader.get_mut();
        write!(stream, "{line}\r\n").unwrap();
        self.read_response()
    }

    fn read_response(&mut self) -> String {
        let mut first_line = String::new();
        self.reader.read_line(&mut first_line).unwrap();

        // "123-" (as opposed to "123 ") starts a multi-line reply that ends
        // with a line starting "123 ".
        if first_line.len() > 3 && first_line.as_bytes()[3] == b'-' {
            let code = &first_line[0..3];
            let terminator = format!("{code} ");
            let mut full = first_line.clone();
            loop {
                let mut line = String::new();
                self.reader.read_line(&mut line).unwrap();
                let done = line.starts_with(&terminator);
                full.push_str(&line);
                if done {
                    break;
                }
            }
            full
        } else {
            first_line
        }
    }

    /// Enters passive mode and returns a connected data-channel stream.
    pub fn passive_data_channel(&mut self) -> TcpStream {
        let response = self.command("PASV");
        assert!(response.starts_with("227"), "PASV failed: {response}");
        let (host, port) = parse_pasv(&response);
        TcpStream::connect((host, port)).unwrap()
    }

    pub fn store(&mut self, remote_name: &str, content: &[u8]) -> String {
        let mut data = self.passive_data_channel();
        let stream = self.reader.get_mut();
        write!(stream, "STOR {remote_name}\r\n").unwrap();
        let opening = self.read_response();
        assert!(opening.starts_with("150"), "STOR did not open a transfer: {opening}");

        data.write_all(content).unwrap();
        data.shutdown(std::net::Shutdown::Write).unwrap();
        drop(data);

        self.read_response()
    }

    pub fn retrieve(&mut self, remote_name: &str) -> (String, Vec<u8>) {
        let mut data = self.passive_data_channel();
        let stream = self.reader.get_mut();
        write!(stream, "RETR {remote_name}\r\n").unwrap();
        let opening = self.read_response();

        let mut content = Vec::new();
        if opening.starts_with("150") {
            data.read_to_end(&mut content).unwrap();
        }
        drop(data);

        (self.read_response(), content)
    }
}

/// Parses a PASV reply like "227 Entering Passive Mode (127,0,0,1,195,80)."
fn parse_pasv(response: &str) -> (String, u16) {
    let start = response.find('(').expect("no ( in PASV response");
    let end = response.find(')').expect("no ) in PASV response");
    let parts: Vec<u16> = response[start + 1..end]
        .split(',')
        .map(|p| p.parse().unwrap())
        .collect();
    let host = format!("{}.{}.{}.{}", parts[0], parts[1], parts[2], parts[3]);
    let port = parts[4] * 256 + parts[5];
    (host, port)
}
