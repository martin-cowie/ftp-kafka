mod auth;
mod kafka;
mod storage;

use auth::TomlAuthenticator;
use kafka::KafkaSendHandler;
use libunftp::ServerBuilder;
use slog::{o, Drain};
use std::sync::Arc;
use storage::MemStorage;


#[tokio::main]
async fn main() {
    // SLog ceremony
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_async::Async::new(slog_term::FullFormat::new(decorator).build().fuse())
        .build()
        .fuse();
    let logger = slog::Logger::root(drain, o!());

    let port: i16 = 2122;

    let authenticator =
        TomlAuthenticator::from_file("config.toml").expect("Failed to load config.toml");

    let server =
        ServerBuilder::with_authenticator(Box::new(|| MemStorage::new()), Arc::new(authenticator))
            .greeting("This is a test FTP server")
            .passive_ports(50000..=65535)
            .site_command("send", KafkaSendHandler)
            .build()
            .expect("Failed to build FTP server");

    let bind_address = format!("0.0.0.0:{}", port);
    slog::info!(logger, "FTP server listening on {}", bind_address);
    server.listen(bind_address).await.expect("Server error");
}
