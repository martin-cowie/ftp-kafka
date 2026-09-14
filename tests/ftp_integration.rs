//! Black-box tests that spawn the real `ftp-server` binary and drive it as
//! an FTP client would, over a real TCP connection. Unlike the unit tests in
//! `src/`, these exercise `main.rs` end to end: CLI parsing, config loading,
//! server startup, and the full command-handling stack.

mod common;

use common::{TestServer, PASSWORD, USERNAME};

#[test]
fn logs_in_with_correct_credentials() {
    let server = TestServer::start();
    let mut client = server.connect();

    let reply = client.login(USERNAME, PASSWORD);
    assert!(reply.starts_with('2'), "expected a 2xx reply, got: {reply}");
}

#[test]
fn rejects_wrong_password() {
    let server = TestServer::start();
    let mut client = server.connect();

    let reply = client.login(USERNAME, "wrong-password");
    assert!(reply.starts_with('5'), "expected a 5xx reply, got: {reply}");
}

#[test]
fn rejects_unknown_user() {
    let server = TestServer::start();
    let mut client = server.connect();

    let reply = client.login("mallory", PASSWORD);
    assert!(reply.starts_with('5'), "expected a 5xx reply, got: {reply}");
}

#[test]
fn stores_and_retrieves_a_file() {
    let server = TestServer::start();
    let mut client = server.connect();
    client.login(USERNAME, PASSWORD);

    let store_reply = client.store("hello.txt", b"hello world");
    assert!(store_reply.starts_with('2'), "STOR failed: {store_reply}");

    let (retrieve_reply, content) = client.retrieve("hello.txt");
    assert!(retrieve_reply.starts_with('2'), "RETR failed: {retrieve_reply}");
    assert_eq!(content, b"hello world");
}

#[test]
fn renames_a_file() {
    let server = TestServer::start();
    let mut client = server.connect();
    client.login(USERNAME, PASSWORD);
    client.store("old.txt", b"content");

    let rnfr = client.command("RNFR old.txt");
    assert!(rnfr.starts_with('3'), "RNFR failed: {rnfr}");
    let rnto = client.command("RNTO new.txt");
    assert!(rnto.starts_with('2'), "RNTO failed: {rnto}");

    let (retrieve_reply, content) = client.retrieve("new.txt");
    assert!(retrieve_reply.starts_with('2'));
    assert_eq!(content, b"content");
}

#[test]
fn deletes_a_file() {
    let server = TestServer::start();
    let mut client = server.connect();
    client.login(USERNAME, PASSWORD);
    client.store("doomed.txt", b"bye");

    let dele = client.command("DELE doomed.txt");
    assert!(dele.starts_with('2'), "DELE failed: {dele}");

    let (retrieve_reply, _) = client.retrieve("doomed.txt");
    assert!(retrieve_reply.starts_with('5'), "expected RETR of a deleted file to fail: {retrieve_reply}");
}

#[test]
fn site_send_without_a_file_name_is_a_syntax_error() {
    // Exercises the SITE SEND command wiring end-to-end without needing a
    // reachable Kafka broker - the argument-parsing failure happens before
    // any Kafka connection is attempted.
    let server = TestServer::start();
    let mut client = server.connect();
    client.login(USERNAME, PASSWORD);

    let reply = client.command("SITE SEND");
    assert!(reply.starts_with('5'), "expected a 5xx syntax error, got: {reply}");
}

#[test]
fn each_session_gets_isolated_storage() {
    let server = TestServer::start();

    let mut first = server.connect();
    first.login(USERNAME, PASSWORD);
    first.store("only-in-first-session.txt", b"secret");

    let mut second = server.connect();
    second.login(USERNAME, PASSWORD);
    let (retrieve_reply, _) = second.retrieve("only-in-first-session.txt");
    assert!(retrieve_reply.starts_with('5'), "second session should not see the first session's files");
}

#[test]
fn second_instance_on_the_same_port_fails_to_start_and_says_why() {
    let server = TestServer::start();
    let mut conflicting = TestServer::start_conflicting(&server);

    let status = conflicting.wait().expect("failed to wait on conflicting instance");
    assert!(!status.success(), "a second instance on the same port should fail to start");

    let mut stderr = String::new();
    use std::io::Read;
    conflicting.stderr.take().unwrap().read_to_string(&mut stderr).unwrap();
    assert!(
        stderr.contains("already in use") || stderr.contains("FTP server failed to listen"),
        "expected a clear reason in stderr, got: {stderr}"
    );
}
