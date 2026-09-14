use async_trait::async_trait;
use clap::Parser;
use libunftp::options::{Reply, ReplyCode, SiteCommandContext, SiteCommandHandler};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use unftp_core::auth::DefaultUser;
use unftp_core::storage::StorageBackend;

use crate::storage::MemStorage;

//FIXME: push to server/principal configuration
const BROKERS: &str = "localhost:9092";
const DEFAULT_TOPIC: &str = "rust-topic"; 

// MARK: Options

#[derive(Parser, Debug)]
#[command(no_binary_name = true)]
struct SendArgs {
    /// Kafka topic to publish to
    #[arg(long, default_value = DEFAULT_TOPIC)]
    topic: String,

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
pub struct KafkaSendHandler;

#[async_trait]
impl SiteCommandHandler<MemStorage, DefaultUser> for KafkaSendHandler {
    async fn handle(&self, context: &SiteCommandContext<MemStorage, DefaultUser>) -> Reply {
        // Get and verify arguments
        let args = match SendArgs::parse(&context.arguments) {
            Ok(args) => args,
            Err(reply) => return reply,
        };

        // Prepare args to get file content
        let Some(user) = context.user.as_ref() else {
            return Reply::new(ReplyCode::NotLoggedIn, "Not logged in");
        };

        // Connect to Kafka
        let producer: FutureProducer = match ClientConfig::new()
            .set("bootstrap.servers", BROKERS)
            .set("message.timeout.ms", "5000")
            .create()
        {
            Ok(producer) => producer,
            Err(e) => {
                return Reply::new(
                    ReplyCode::LocalError,
                    &format!("Cannot connect to Kafka broker {}: {}", BROKERS, e),
                );
            }
        };
        slog::info!(context.logger, "Connected to {}", BROKERS);

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
            let message = FutureRecord::to(&args.topic).key(key).payload(&payload);
            match producer.send(message, Duration::from_secs(0)).await {
                Ok(delivery) => results.push(format!(
                    "Sent file \"{}\" as message to {}. {:?}",
                    file_name, args.topic, delivery
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
    use std::sync::Arc;

    use crate::storage::MemStorage;

    fn context(arguments: &str, user: Option<DefaultUser>, storage: MemStorage) -> SiteCommandContext<MemStorage, DefaultUser> {
        SiteCommandContext {
            command: "SEND".to_string(),
            arguments: arguments.to_string(),
            username: user.as_ref().map(|_| "alice".to_string()),
            storage: Arc::new(storage),
            user: Arc::new(user),
            storage_features: 0,
            logger: slog::Logger::root(slog::Discard, slog::o!()),
        }
    }

    fn is_code(reply: &Reply, expected: ReplyCode) -> bool {
        matches!(reply, Reply::CodeAndMsg { code, .. } if *code == expected)
            || matches!(reply, Reply::MultiLine { code, .. } if *code == expected)
    }

    #[tokio::test]
    async fn handle_rejects_missing_file_names_without_touching_storage_or_kafka() {
        let ctx = context("", None, MemStorage::new());
        let reply = KafkaSendHandler.handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::ParameterSyntaxError));
    }

    #[tokio::test]
    async fn handle_rejects_when_not_logged_in() {
        let ctx = context("hello.txt", None, MemStorage::new());
        let reply = KafkaSendHandler.handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::NotLoggedIn));
    }

    #[tokio::test]
    async fn handle_reports_missing_files_per_name_without_a_reachable_broker() {
        let ctx = context("missing.txt", Some(DefaultUser), MemStorage::new());
        let reply = KafkaSendHandler.handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::LocalError));
    }


    #[test]
    fn parses_plain_file_names() {
        let args = SendArgs::parse("file1.txt file2.txt").unwrap();
        assert_eq!(args.file_names, vec!["file1.txt", "file2.txt"]);
        assert_eq!(args.topic, DEFAULT_TOPIC);
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
        assert_eq!(args.topic, "my-topic");
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
