mod auth;
mod storage;

use auth::TomlAuthenticator;
use libunftp::ServerBuilder;
use storage::MemStorage;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let authenticator = TomlAuthenticator::from_file("config.toml")
        .expect("Failed to load config.toml");

    let server = ServerBuilder::with_authenticator(
        Box::new(|| MemStorage::new()),
        Arc::new(authenticator),
    )
    .greeting("this is a test FTP server")
    .passive_ports(50000..=65535)
    .build()
    .expect("Failed to build FTP server");

    println!("FTP server listening on 0.0.0.0:2121");
    server.listen("0.0.0.0:2121").await.expect("Server error");
}
