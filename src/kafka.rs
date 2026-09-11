use async_trait::async_trait;
use clap::Parser;
use libunftp::options::{Reply, ReplyCode, SiteCommandContext, SiteCommandHandler};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Deserialize;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use unftp_core::auth::DefaultUser;
use unftp_core::storage::StorageBackend;

use crate::storage::MemStorage;

// MARK: Config

/// Kafka settings, loaded from the `[kafka]` table in `config.toml`. Any
/// field left out of the file falls back to its default here.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    pub brokers: String,
    pub default_topic: String,
    pub message_timeout_ms: u64,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        KafkaConfig {
            brokers: "localhost:9092".to_string(),
            default_topic: "rust-topic".to_string(),
            message_timeout_ms: 5000,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    kafka: KafkaConfig,
}

impl KafkaConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let config: ConfigFile = toml::from_str(&content)?;
        Ok(config.kafka)
    }
}

// MARK: Options

#[derive(Parser, Debug)]
#[command(no_binary_name = true)]
struct SendArgs {
    /// Kafka topic to publish to (defaults to the configured default_topic)
    #[arg(long)]
    topic: Option<String>,

    /// Kafka message key (defaults to each file's name)
    #[arg(long)]
    key: Option<String>,

    file_names: Vec<String>,
}

impl SendArgs {
    fn parse(raw: &str) -> Result<Self, Reply> {
        let Some(tokens) = shlex::split(raw) else {
            return Err(Reply::new(
                ReplyCode::ParameterSyntaxError,
                "Unbalanced quotes in arguments",
            ));
        };
        let args = Self::try_parse_from(tokens)
            .map_err(|e| Reply::new(ReplyCode::ParameterSyntaxError, &e.to_string()))?;
        if args.file_names.is_empty() {
            return Err(Reply::new(ReplyCode::ParameterSyntaxError, "Missing file names"));
        }
        Ok(args)
    }
}

// MARK: Implementation

#[derive(Debug)]
pub struct KafkaSendHandler {
    config: KafkaConfig,
}

impl KafkaSendHandler {
    pub fn new(config: KafkaConfig) -> Self {
        KafkaSendHandler { config }
    }
}

#[async_trait]
impl SiteCommandHandler<MemStorage, DefaultUser> for KafkaSendHandler {
    async fn handle(&self, context: &SiteCommandContext<MemStorage, DefaultUser>) -> Reply {
        // Get and verify arguments
        let args = match SendArgs::parse(&context.arguments) {
            Ok(args) => args,
            Err(reply) => return reply,
        };
        let topic = args.topic.as_deref().unwrap_or(&self.config.default_topic);

        // Prepare args to get file content
        let Some(user) = context.user.as_ref() else {
            return Reply::new(ReplyCode::NotLoggedIn, "Not logged in");
        };

        // Connect to Kafka
        let producer: FutureProducer = match ClientConfig::new()
            .set("bootstrap.servers", &self.config.brokers)
            .set("message.timeout.ms", self.config.message_timeout_ms.to_string())
            .create()
        {
            Ok(producer) => producer,
            Err(e) => {
                return Reply::new(
                    ReplyCode::LocalError,
                    &format!("Cannot connect to Kafka broker {}: {}", self.config.brokers, e),
                );
            }
        };
        slog::info!(context.logger, "Connected to {}", self.config.brokers);

        // Send each file as a discrete message
        let mut results = Vec::new();
        let mut all_ok = true;
        for file_name in &args.file_names {
            //FIXME: unnecessary copy
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

            let key = args.key.as_deref().unwrap_or(file_name);
            let message = FutureRecord::to(topic).key(key).payload(&payload);
            match producer.send(message, Duration::from_secs(0)).await {
                Ok(delivery) => results.push(format!(
                    "Sent file \"{}\" as message to {}. {:?}",
                    file_name, topic, delivery
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

// MARK: Tests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_config_defaults_when_section_is_absent() {
        let config: ConfigFile = toml::from_str("").unwrap();
        assert_eq!(config.kafka.brokers, "localhost:9092");
        assert_eq!(config.kafka.default_topic, "rust-topic");
        assert_eq!(config.kafka.message_timeout_ms, 5000);
    }

    #[test]
    fn kafka_config_reads_every_field() {
        let config: ConfigFile = toml::from_str(
            r#"
                [kafka]
                brokers = "kafka1:9092,kafka2:9092"
                default_topic = "custom-topic"
                message_timeout_ms = 10000
            "#,
        )
        .unwrap();
        assert_eq!(config.kafka.brokers, "kafka1:9092,kafka2:9092");
        assert_eq!(config.kafka.default_topic, "custom-topic");
        assert_eq!(config.kafka.message_timeout_ms, 10000);
    }

    #[test]
    fn kafka_config_falls_back_per_field() {
        let config: ConfigFile = toml::from_str(
            r#"
                [kafka]
                brokers = "my-broker:9092"
            "#,
        )
        .unwrap();
        assert_eq!(config.kafka.brokers, "my-broker:9092");
        assert_eq!(config.kafka.default_topic, "rust-topic");
        assert_eq!(config.kafka.message_timeout_ms, 5000);
    }

    #[test]
    fn kafka_config_from_file_reads_the_kafka_table() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(
            &mut file,
            br#"
                [kafka]
                brokers = "my-broker:9092"

                [[users]]
                username = "alice"
                password = "password123"
            "#,
        )
        .unwrap();

        let config = KafkaConfig::from_file(file.path().to_str().unwrap()).unwrap();
        assert_eq!(config.brokers, "my-broker:9092");
        assert_eq!(config.default_topic, "rust-topic");
    }

    #[test]
    fn kafka_config_from_file_fails_when_file_missing() {
        let result = KafkaConfig::from_file("/nonexistent/path/config.toml");
        assert!(result.is_err());
    }

    #[test]
    fn parses_plain_file_names() {
        let args = SendArgs::parse("file1.txt file2.txt").unwrap();
        assert_eq!(args.file_names, vec!["file1.txt", "file2.txt"]);
        assert_eq!(args.topic, None);
        assert_eq!(args.key, None);
    }

    #[test]
    fn parses_quoted_file_name_with_embedded_spaces() {
        let args = SendArgs::parse("\"my file.txt\"").unwrap();
        assert_eq!(args.file_names, vec!["my file.txt"]);
    }

    #[test]
    fn parses_topic_and_key_alongside_quoted_file_names() {
        let args =
            SendArgs::parse("--topic my-topic --key my-key \"my file.txt\" other.txt").unwrap();
        assert_eq!(args.topic, Some("my-topic".to_string()));
        assert_eq!(args.key, Some("my-key".to_string()));
        assert_eq!(args.file_names, vec!["my file.txt", "other.txt"]);
    }

    #[test]
    fn rejects_unbalanced_quotes() {
        let err = SendArgs::parse("\"unterminated").unwrap_err();
        assert!(matches!(
            err,
            Reply::CodeAndMsg {
                code: ReplyCode::ParameterSyntaxError,
                ..
            }
        ));
    }

    #[test]
    fn rejects_missing_file_names() {
        let err = SendArgs::parse("--topic my-topic").unwrap_err();
        assert!(matches!(
            err,
            Reply::CodeAndMsg {
                code: ReplyCode::ParameterSyntaxError,
                ..
            }
        ));
    }
}
