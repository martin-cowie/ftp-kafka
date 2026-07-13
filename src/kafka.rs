use async_trait::async_trait;
use libunftp::options::{Reply, ReplyCode, SiteCommandContext, SiteCommandHandler};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use unftp_core::auth::DefaultUser;
use unftp_core::storage::StorageBackend;

use crate::storage::MemStorage;

const BROKERS: &str = "localhost:9092";
const TOPIC: &str = "rust-topic";

#[derive(Debug)]
pub struct KafkaSendHandler;

#[async_trait]
impl SiteCommandHandler<MemStorage, DefaultUser> for KafkaSendHandler {
    async fn handle(&self, context: &SiteCommandContext<MemStorage, DefaultUser>) -> Reply {
        // Get and verify arguments
        let file_names: Vec<&str> = context
            .arguments
            .split(' ')
            .filter(|name| !name.is_empty())
            .collect();
        if file_names.is_empty() {
            return Reply::new(ReplyCode::ParameterSyntaxError, "Missing file names");
        }

        // Prepare args to get file content
        let Some(user) = context.user.as_ref() else {
            return Reply::new(ReplyCode::NotLoggedIn, "Not logged in");
        };

        // Connect to Kafka
        let producer: &FutureProducer = &ClientConfig::new()
            .set("bootstrap.servers", BROKERS)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");
        slog::info!(context.logger, "Connected to {}", BROKERS);

        // Send each file as a discrete message
        let mut results = Vec::new();
        let mut all_ok = true;
        for file_name in file_names {
            let mut reader = match context.storage.get(user, file_name, 0).await {
                Ok(reader) => reader,
                Err(e) => {
                    results.push(format!("\"{}\": {:?}", file_name, e));
                    all_ok = false;
                    continue;
                }
            };

            let mut payload = Vec::new();
            if let Err(e) = reader.read_to_end(&mut payload).await {
                results.push(format!("\"{}\": {:?}", file_name, e));
                all_ok = false;
                continue;
            };

            let message = FutureRecord::to(TOPIC).key(file_name).payload(&payload);
            match producer.send(message, Duration::from_secs(0)).await {
                Ok(delivery) => results.push(format!(
                    "Sent file \"{}\" as message to {}. {:?}",
                    file_name, TOPIC, delivery
                )),
                Err((kerr, _)) => {
                    results.push(format!("\"{}\": {:?}", file_name, kerr));
                    all_ok = false;
                }
            }
        }

        let reply_code = if all_ok {
            ReplyCode::CommandOkay
        } else {
            ReplyCode::LocalError
        };
        Reply::new_multiline(reply_code, results)
    }
}
