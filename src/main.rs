mod auth;
mod storage;

use async_trait::async_trait;
use auth::TomlAuthenticator;
use libunftp::ServerBuilder;
use libunftp::options::{Reply, ReplyCode, SiteCommandContext, SiteCommandHandler};
use storage::MemStorage;
use std::sync::Arc;

#[derive(Debug)]
struct SiteTestHandler;

#[async_trait]
impl SiteCommandHandler for SiteTestHandler {
    async fn handle(&self, _context: SiteCommandContext) -> Reply {
        Reply::new(ReplyCode::CommandOkay, "Hello world")
    }
}

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
    .site_command("TEST", SiteTestHandler)
    .build()
    .expect("Failed to build FTP server");

    println!("FTP server listening on 0.0.0.0:2121");
    server.listen("0.0.0.0:2121").await.expect("Server error");
}
