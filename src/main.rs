mod auth;
mod config;
mod kafka;
mod server_config;
mod storage;

use auth::KafkaAuthenticator;
use clap::Parser;
use config::Config;
use kafka::{KafkaSendHandler, KafkaTopicHandler, KafkaTopicsHandler, SecurityProtocol};
use libunftp::ServerBuilder;
use server_config::LogLevel;
use slog::{o, Drain};
use std::sync::Arc;
use storage::MemStorage;

#[derive(Parser)]
struct Cli {
    /// Path to the configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();

    let config = match Config::from_path(&cli.config) {
        Ok(config) => config,
        Err(e) => {
            eprintln!(
                "Cannot load {}: {}",
                cli.config.display(),
                describe_error(&*e)
            );
            return std::process::ExitCode::FAILURE;
        }
    };
    let logger = make_logger(config.server.log_level);
    let port = config.server.port;
    let kafka_config = config.kafka;

    if kafka_config.security_protocol == SecurityProtocol::Plaintext {
        slog::warn!(
            logger,
            "security_protocol is PLAINTEXT: FTP logins are not authenticated, any username and password is accepted"
        );
    }

    let authenticator = KafkaAuthenticator::new(kafka_config, logger.clone());
    let user_detail_provider = authenticator.user_detail_provider();

    let server =
        match ServerBuilder::with_authenticator(Box::new(MemStorage::new), Arc::new(authenticator))
            .user_detail_provider(Arc::new(user_detail_provider))
            .logger(logger.clone())
            .greeting("This is a test FTP server")
            .passive_ports(50000..=65535)
            .site_command("send", KafkaSendHandler)
            .site_command("topics", KafkaTopicsHandler)
            .site_command("topic", KafkaTopicHandler)
            .build()
        {
            Ok(server) => server,
            Err(e) => {
                slog::error!(logger, "Cannot build FTP server: {}", describe_error(&e));
                return std::process::ExitCode::FAILURE;
            }
        };

    let bind_address = format!("0.0.0.0:{}", port);
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

fn make_logger(level: LogLevel) -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_async::Async::new(slog_term::FullFormat::new(decorator).build().fuse())
        .build()
        .filter_level(level.as_slog())
        .fuse();
    slog::Logger::root(drain, o!())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_path_defaults_to_config_toml() {
        let cli = Cli::try_parse_from(["ftp-server"]).unwrap();
        assert_eq!(cli.config, std::path::PathBuf::from("config.toml"));
    }

    #[test]
    fn config_path_can_be_given_on_the_command_line() {
        let cli = Cli::try_parse_from(["ftp-server", "--config", "/etc/ftp-kafka.toml"]).unwrap();
        assert_eq!(cli.config, std::path::PathBuf::from("/etc/ftp-kafka.toml"));
    }
}
