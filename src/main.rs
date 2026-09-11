mod auth;
mod kafka;
mod storage;

use auth::TomlAuthenticator;
use clap::Parser;
use kafka::{KafkaConfig, KafkaSendHandler};
use libunftp::ServerBuilder;
use slog::{o, Drain};
use std::sync::Arc;
use storage::MemStorage;

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value_t = 2121)]
    port: u16,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();

    // SLog ceremony
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_async::Async::new(slog_term::FullFormat::new(decorator).build().fuse())
        .build()
        .fuse();
    let logger = slog::Logger::root(drain, o!());

    let authenticator =
        TomlAuthenticator::from_file("config.toml").expect("Failed to load config.toml");
    let kafka_config =
        KafkaConfig::from_file("config.toml").expect("Failed to load config.toml");

    let server = match ServerBuilder::with_authenticator(
        Box::new(|| MemStorage::new()),
        Arc::new(authenticator),
    )
    .greeting("This is a test FTP server")
    .passive_ports(50000..=65535)
    .site_command("send", KafkaSendHandler::new(kafka_config))
    .build()
    {
        Ok(server) => server,
        Err(e) => {
            slog::error!(logger, "Cannot build FTP server: {}", describe_error(&e));
            return std::process::ExitCode::FAILURE;
        }
    };

    let bind_address = format!("0.0.0.0:{}", cli.port);
    slog::info!(logger, "FTP server listening on {}", bind_address);
    if let Err(e) = server.listen(bind_address.clone()).await {
        slog::error!(
            logger,
            "FTP server failed to listen on {}: {}",
            bind_address,
            describe_error(&e)
        );
        return std::process::ExitCode::FAILURE;
    }

    std::process::ExitCode::SUCCESS
}

/// Renders an error together with its full source chain, e.g.
/// "server error: io error: Address already in use (os error 48)".
fn describe_error(err: &(dyn std::error::Error + 'static)) -> String {
    let mut msg = err.to_string();
    let mut source = err.source();
    while let Some(e) = source {
        msg.push_str(": ");
        msg.push_str(&e.to_string());
        source = e.source();
    }
    msg
}
