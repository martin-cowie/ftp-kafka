mod auth;
mod storage;

use async_trait::async_trait;
use auth::TomlAuthenticator;
use libunftp::options::{Reply, ReplyCode, SiteCommandContext, SiteCommandHandler};
use libunftp::ServerBuilder;
use rdkafka::config::ClientConfig;
// use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::Duration;
use storage::MemStorage;
use unftp_core::auth::DefaultUser;

const BROKERS: &str = "localhost:9092";
const TOPIC: &str = "rust-topic";

#[derive(Debug)]
struct SiteTestHandler;

#[async_trait]
impl SiteCommandHandler<MemStorage, DefaultUser> for SiteTestHandler {
    async fn handle(&self, context: &SiteCommandContext<MemStorage, DefaultUser>) -> Reply {
        let username = context.username.as_deref().unwrap_or("anon");
        let mut lines = vec![format!("Hello {username}")];
        lines.extend(context.storage.file_names().into_iter().map(|name| name.to_uppercase()));
        Reply::new_multiline(ReplyCode::CommandOkay, lines)
    }
}

#[derive(Debug)]
struct KafkaSendHandler;

#[async_trait]
impl SiteCommandHandler<MemStorage, DefaultUser> for KafkaSendHandler {
    async fn handle(&self, context: &SiteCommandContext<MemStorage, DefaultUser>) -> Reply {
        let mut args = context.arguments.split(' ');
        let (Some(file_name), None) = (args.next(), args.next()) else {
            return Reply::new(ReplyCode::ParameterSyntaxError, "Missing file name");
        };

        let payload = String::from("pretend file content");

        let producer: &FutureProducer = &ClientConfig::new()
            .set("bootstrap.servers", BROKERS)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");
        slog::info!(context.logger, "Connected to {}", BROKERS);

        // let payload = format!("Message {}", 42);
        let message = FutureRecord::to(TOPIC)
            .key(file_name)
            .payload(&payload);

        match producer.send(message, Duration::from_secs(0),).await {
            Ok(_) => Reply::new(ReplyCode::CommandOkay, &format!("Send message to {} on {}", TOPIC, BROKERS)),
            Err((kerr, _)) => Reply::new(ReplyCode::LocalError, &format!("{:?}", kerr)),
        }
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
        .greeting("This is a test FTP server")
        .passive_ports(50000..=65535)
        .site_command("test", SiteTestHandler)
        .site_command("send", KafkaSendHandler)
        .build()
        .expect("Failed to build FTP server");

    println!("FTP server listening on 0.0.0.0:2121");
    server.listen("0.0.0.0:2121").await.expect("Server error");
}
