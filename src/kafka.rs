use async_trait::async_trait;
use clap::Parser;
use libunftp::options::{Reply, ReplyCode, SiteCommandContext, SiteCommandHandler};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureRecord, Producer};
use serde::Deserialize;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use unftp_core::storage::StorageBackend;

use crate::auth::KafkaUser;
use crate::storage::MemStorage;

// MARK: Config

/// Kafka settings, loaded from the required `[kafka]` table in `config.toml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "RawKafkaConfig")]
pub struct KafkaConfig {
    /// Comma-separated `host:port` list, passed to librdkafka as `bootstrap.servers`.
    pub brokers: String,
    /// Each Session's starting topic, used when `SITE SEND` has no `--topic`.
    pub default_topic: String,
    /// How long to wait for a message's delivery.
    pub message_timeout_ms: u64,
    /// How long a login waits for the cluster to accept or reject the credentials.
    pub auth_timeout_ms: u64,
    /// How the bridge talks to the cluster.
    pub security_protocol: SecurityProtocol,
    /// SASL mechanism; present exactly when `security_protocol` is `SASL_PLAINTEXT`.
    pub sasl_mechanism: Option<SaslMechanism>,
}

/// The librdkafka `security.protocol` values the bridge supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
pub enum SecurityProtocol {
    #[default]
    #[serde(rename = "PLAINTEXT")]
    Plaintext,
    #[serde(rename = "SASL_PLAINTEXT")]
    SaslPlaintext,
    //TODO SSL/TLS editions of both of these - see https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html
}

impl SecurityProtocol {
    /// The value librdkafka expects for `security.protocol`.
    pub fn as_librdkafka(self) -> &'static str {
        match self {
            SecurityProtocol::Plaintext => "PLAINTEXT",
            SecurityProtocol::SaslPlaintext => "SASL_PLAINTEXT",
        }
    }
}

/// The librdkafka `sasl.mechanism` values the bridge supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum SaslMechanism {
    #[serde(rename = "PLAIN")]
    Plain,
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
    //TODO: GSSAPI and OAUTHBEARER
}

impl SaslMechanism {
    /// The value librdkafka expects for `sasl.mechanism`.
    pub fn as_librdkafka(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// The `[kafka]` table as written, before cross-field validation.
#[derive(Debug, Deserialize)]
struct RawKafkaConfig {
    brokers: String,
    default_topic: String,
    #[serde(default = "KafkaConfig::default_message_timeout_ms")]
    message_timeout_ms: u64,
    #[serde(default = "KafkaConfig::default_auth_timeout_ms")]
    auth_timeout_ms: u64,
    #[serde(default)]
    security_protocol: SecurityProtocol,
    sasl_mechanism: Option<SaslMechanism>,
}

impl TryFrom<RawKafkaConfig> for KafkaConfig {
    type Error = String;

    fn try_from(raw: RawKafkaConfig) -> Result<Self, Self::Error> {
        if raw.security_protocol == SecurityProtocol::SaslPlaintext && raw.sasl_mechanism.is_none()
        {
            return Err(
                "sasl_mechanism is required when security_protocol is SASL_PLAINTEXT".to_string(),
            );
        }
        Ok(KafkaConfig {
            brokers: raw.brokers,
            default_topic: raw.default_topic,
            message_timeout_ms: raw.message_timeout_ms,
            auth_timeout_ms: raw.auth_timeout_ms,
            security_protocol: raw.security_protocol,
            sasl_mechanism: raw.sasl_mechanism,
        })
    }
}

impl KafkaConfig {
    fn default_message_timeout_ms() -> u64 {
        5000
    }

    fn default_auth_timeout_ms() -> u64 {
        5000
    }
}

// MARK: Options

#[derive(Parser, Debug)]
#[command(no_binary_name = true)]
struct SendArgs {
    /// Kafka topic to publish to (defaults to the Session topic)
    #[arg(long)]
    topic: Option<String>,

    /// Kafka message key (defaults to each file's name)
    #[arg(long)]
    key: Option<String>,

    /// Kafka message header as NAME=VALUE; may be repeated
    #[arg(long = "header", value_name = "NAME=VALUE", value_parser = SendArgs::parse_header)]
    headers: Vec<(String, String)>,

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
            return Err(Reply::new(
                ReplyCode::ParameterSyntaxError,
                "Missing file names",
            ));
        }
        Ok(args)
    }

    /// Parses a `NAME=VALUE` header argument, splitting on the first `=` so the value may contain `=`.
    ///
    /// # Errors
    /// Returns a message when there is no `=` or the name is empty.
    fn parse_header(raw: &str) -> Result<(String, String), String> {
        match raw.split_once('=') {
            Some((name, value)) if !name.is_empty() => Ok((name.to_string(), value.to_string())),
            _ => Err(format!("expected NAME=VALUE, got \"{raw}\"")),
        }
    }
}

/// Builds a message's headers from the command line's `--header` pairs, in order.
fn make_headers(pairs: &[(String, String)]) -> OwnedHeaders {
    pairs
        .iter()
        .fold(OwnedHeaders::new(), |headers, (name, value)| {
            headers.insert(Header {
                key: name,
                value: Some(value.as_str()),
            })
        })
}

// MARK: Implementation

/// How long `SITE TOPICS` and `SITE TOPIC <topic>` wait for the cluster's metadata.
const METADATA_TIMEOUT: Duration = Duration::from_secs(5);

/// Splits a SITE command's arguments as a shell would.
fn split_arguments(raw: &str) -> Result<Vec<String>, Reply> {
    shlex::split(raw).ok_or_else(|| {
        Reply::new(
            ReplyCode::ParameterSyntaxError,
            "Unbalanced quotes in arguments",
        )
    })
}

/// Lists the topics on the cluster, as seen with the Session's Kafka credentials.
///
/// Internal topics, whose names start with `__` (such as `__consumer_offsets`),
/// are left out. The names come back sorted.
async fn list_topics(user: &KafkaUser) -> Result<Vec<String>, Reply> {
    let producer = user.producer.clone();
    let metadata = tokio::task::spawn_blocking(move || {
        producer.client().fetch_metadata(None, METADATA_TIMEOUT)
    })
    .await
    .map_err(|e| Reply::new(ReplyCode::LocalError, &format!("Cannot list topics: {e}")))?
    .map_err(|e| Reply::new(ReplyCode::LocalError, &format!("Cannot list topics: {e}")))?;
    let mut topics: Vec<String> = metadata
        .topics()
        .iter()
        .filter(|topic| topic.error().is_none() && !topic.name().starts_with("__"))
        .map(|topic| topic.name().to_string())
        .collect();
    topics.sort();
    Ok(topics)
}

/// `SITE SEND`: publishes uploaded files to Kafka.
#[derive(Debug, Default)]
pub struct KafkaSendHandler;

#[async_trait]
impl SiteCommandHandler<MemStorage, KafkaUser> for KafkaSendHandler {
    async fn handle(&self, context: &SiteCommandContext<MemStorage, KafkaUser>) -> Reply {
        // Get and verify arguments
        let args = match SendArgs::parse(&context.arguments) {
            Ok(args) => args,
            Err(reply) => return reply,
        };

        // Prepare args to get file content
        let Some(user) = context.user.as_ref() else {
            return Reply::new(ReplyCode::NotLoggedIn, "Not logged in");
        };

        let topic = args.topic.clone().unwrap_or_else(|| user.topic());
        let topic = topic.as_str();
        let producer = &user.producer;

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
            let message = FutureRecord::to(topic)
                .key(key)
                .payload(&payload)
                .headers(make_headers(&args.headers));
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

/// `SITE TOPICS`: lists the topics on the cluster, one per line.
#[derive(Debug, Default)]
pub struct KafkaTopicsHandler;

#[async_trait]
impl SiteCommandHandler<MemStorage, KafkaUser> for KafkaTopicsHandler {
    async fn handle(&self, context: &SiteCommandContext<MemStorage, KafkaUser>) -> Reply {
        match split_arguments(&context.arguments) {
            Ok(args) if args.is_empty() => {}
            Ok(_) => {
                return Reply::new(
                    ReplyCode::ParameterSyntaxError,
                    "SITE TOPICS takes no arguments",
                )
            }
            Err(reply) => return reply,
        }
        let Some(user) = context.user.as_ref() else {
            return Reply::new(ReplyCode::NotLoggedIn, "Not logged in");
        };
        let topics = match list_topics(user).await {
            Ok(topics) => topics,
            Err(reply) => return reply,
        };
        let mut lines = Vec::with_capacity(topics.len() + 2);
        lines.push(format!("{} topic(s):", topics.len()));
        lines.extend(topics);
        lines.push("End of topics".to_string());
        Reply::new_multiline(ReplyCode::CommandOkay, lines)
    }
}

/// `SITE TOPIC [<topic>]`: reports or sets the Session topic.
#[derive(Debug, Default)]
pub struct KafkaTopicHandler;

#[async_trait]
impl SiteCommandHandler<MemStorage, KafkaUser> for KafkaTopicHandler {
    async fn handle(&self, context: &SiteCommandContext<MemStorage, KafkaUser>) -> Reply {
        let args = match split_arguments(&context.arguments) {
            Ok(args) if args.len() <= 1 => args,
            Ok(_) => {
                return Reply::new(
                    ReplyCode::ParameterSyntaxError,
                    "Usage: SITE TOPIC [<topic>]",
                )
            }
            Err(reply) => return reply,
        };
        let Some(user) = context.user.as_ref() else {
            return Reply::new(ReplyCode::NotLoggedIn, "Not logged in");
        };
        let Some(topic) = args.into_iter().next() else {
            return Reply::new(
                ReplyCode::CommandOkay,
                &format!("Session topic is {}", user.topic()),
            );
        };

        // Validate against the cluster rather than rely on auto-creation.
        let topics = match list_topics(user).await {
            Ok(topics) => topics,
            Err(reply) => return reply,
        };
        if !topics.contains(&topic) {
            return Reply::new(
                ReplyCode::FileError,
                &format!("Topic {topic} does not exist"),
            );
        }
        user.set_topic(topic.clone());
        Reply::new(
            ReplyCode::CommandOkay,
            &format!("Session topic set to {topic}"),
        )
    }
}

// MARK: Tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::SessionProducer;
    use crate::storage::MemStorage;
    use rdkafka::config::ClientConfig;
    use rdkafka::message::Headers;
    use std::sync::Arc;

    /// A `config.toml` holding only the `[kafka]` table.
    #[derive(Debug, Deserialize)]
    struct Config {
        kafka: KafkaConfig,
    }

    fn session_user() -> KafkaUser {
        let producer: SessionProducer = ClientConfig::new()
            .set("bootstrap.servers", "127.0.0.1:1")
            .create_with_context(crate::auth::AuthContext::default())
            .unwrap();
        KafkaUser::new("alice".to_string(), producer, "test-topic".to_string())
    }

    fn context(
        arguments: &str,
        user: Option<KafkaUser>,
        storage: MemStorage,
    ) -> SiteCommandContext<MemStorage, KafkaUser> {
        SiteCommandContext {
            command: "SEND".to_string(),
            arguments: arguments.to_string(),
            username: user.as_ref().map(|_| "alice".to_string()),
            storage: Arc::new(storage),
            user: Arc::new(user),
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
        let ctx = context("missing.txt", Some(session_user()), MemStorage::new());
        let reply = KafkaSendHandler.handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::LocalError));
    }

    #[tokio::test]
    async fn topic_reports_the_session_topic() {
        let ctx = context("", Some(session_user()), MemStorage::new());
        let reply = KafkaTopicHandler.handle(&ctx).await;
        let Reply::CodeAndMsg { code, msg } = reply else {
            panic!("expected a single-line reply, got {reply:?}");
        };
        assert_eq!(code, ReplyCode::CommandOkay);
        assert!(msg.contains("test-topic"), "{msg}");
    }

    #[tokio::test]
    async fn topic_rejects_more_than_one_argument() {
        let ctx = context("a b", Some(session_user()), MemStorage::new());
        let reply = KafkaTopicHandler.handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::ParameterSyntaxError));
    }

    #[tokio::test]
    async fn topic_and_topics_reject_when_not_logged_in() {
        let ctx = context("", None, MemStorage::new());
        assert!(is_code(
            &KafkaTopicHandler.handle(&ctx).await,
            ReplyCode::NotLoggedIn
        ));
        assert!(is_code(
            &KafkaTopicsHandler.handle(&ctx).await,
            ReplyCode::NotLoggedIn
        ));
    }

    #[tokio::test]
    async fn topics_rejects_arguments() {
        let ctx = context("extra", Some(session_user()), MemStorage::new());
        let reply = KafkaTopicsHandler.handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::ParameterSyntaxError));
    }

    #[test]
    fn kafka_config_requires_the_kafka_table() {
        let err = toml::from_str::<Config>("").unwrap_err();
        assert!(err.to_string().contains("kafka"), "{err}");
    }

    #[test]
    fn kafka_config_requires_brokers() {
        let err = toml::from_str::<Config>(
            r#"
                [kafka]
                default_topic = "custom-topic"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("brokers"), "{err}");
    }

    #[test]
    fn kafka_config_requires_default_topic() {
        let err = toml::from_str::<Config>(
            r#"
                [kafka]
                brokers = "my-broker:9092"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("default_topic"), "{err}");
    }

    #[test]
    fn kafka_config_reads_every_field() {
        let config: Config = toml::from_str(
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
    fn kafka_config_message_timeout_defaults_to_5000() {
        let config: Config = toml::from_str(
            r#"
                [kafka]
                brokers = "my-broker:9092"
                default_topic = "custom-topic"
            "#,
        )
        .unwrap();
        assert_eq!(config.kafka.message_timeout_ms, 5000);
    }

    #[test]
    fn kafka_config_security_protocol_defaults_to_plaintext() {
        let config: Config = toml::from_str(
            r#"
                [kafka]
                brokers = "my-broker:9092"
                default_topic = "custom-topic"
            "#,
        )
        .unwrap();
        assert_eq!(config.kafka.security_protocol, SecurityProtocol::Plaintext);
    }

    #[test]
    fn kafka_config_auth_timeout_defaults_to_5000_and_can_be_overridden() {
        let default: Config = toml::from_str(
            r#"
                [kafka]
                brokers = "my-broker:9092"
                default_topic = "custom-topic"
            "#,
        )
        .unwrap();
        let overridden: Config = toml::from_str(
            r#"
                [kafka]
                brokers = "my-broker:9092"
                default_topic = "custom-topic"
                auth_timeout_ms = 1500
            "#,
        )
        .unwrap();
        assert_eq!(default.kafka.auth_timeout_ms, 5000);
        assert_eq!(overridden.kafka.auth_timeout_ms, 1500);
    }

    #[test]
    fn kafka_config_rejects_unsupported_security_protocols_and_mechanisms() {
        for (protocol, mechanism) in [
            ("SSL", "PLAIN"),
            ("SASL_SSL", "PLAIN"),
            ("SASL_PLAINTEXT", "GSSAPI"),
            ("SASL_PLAINTEXT", "OAUTHBEARER"),
        ] {
            let result = toml::from_str::<Config>(&format!(
                "[kafka]\nbrokers = \"b:9092\"\ndefault_topic = \"t\"\nsecurity_protocol = \"{protocol}\"\nsasl_mechanism = \"{mechanism}\"\n"
            ));
            assert!(
                result.is_err(),
                "{protocol} with {mechanism} should be rejected"
            );
        }
    }

    #[test]
    fn kafka_config_accepts_each_supported_sasl_mechanism() {
        for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] {
            let config: Config = toml::from_str(&format!(
                "[kafka]\nbrokers = \"b:9092\"\ndefault_topic = \"t\"\nsecurity_protocol = \"SASL_PLAINTEXT\"\nsasl_mechanism = \"{mechanism}\"\n"
            ))
            .unwrap();
            assert!(config.kafka.sasl_mechanism.is_some(), "{mechanism}");
        }
    }

    #[test]
    fn kafka_config_sasl_plaintext_requires_a_sasl_mechanism() {
        let err = toml::from_str::<Config>(
            r#"
                [kafka]
                brokers = "my-broker:9092"
                default_topic = "custom-topic"
                security_protocol = "SASL_PLAINTEXT"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("sasl_mechanism"), "{err}");
    }

    #[test]
    fn parses_plain_file_names() {
        let args = SendArgs::parse("file1.txt file2.txt").unwrap();
        assert_eq!(args.file_names, vec!["file1.txt", "file2.txt"]);
        assert_eq!(args.topic, None);
        assert_eq!(args.key, None);
        assert!(args.headers.is_empty());
    }

    #[test]
    fn parses_repeated_headers_in_order() {
        let args = SendArgs::parse("--header a=1 --header \"b=two words\" f.txt").unwrap();
        assert_eq!(
            args.headers,
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "two words".to_string()),
            ]
        );
        assert_eq!(args.file_names, vec!["f.txt"]);
    }

    #[test]
    fn header_value_may_contain_equals() {
        assert_eq!(
            SendArgs::parse_header("a=b=c"),
            Ok(("a".to_string(), "b=c".to_string()))
        );
    }

    #[test]
    fn header_value_may_be_empty() {
        assert_eq!(
            SendArgs::parse_header("a="),
            Ok(("a".to_string(), String::new()))
        );
    }

    #[test]
    fn rejects_header_without_equals() {
        let err = SendArgs::parse("--header noequals f.txt").unwrap_err();
        assert!(matches!(
            err,
            Reply::CodeAndMsg {
                code: ReplyCode::ParameterSyntaxError,
                ..
            }
        ));
    }

    #[test]
    fn rejects_header_with_empty_name() {
        assert!(SendArgs::parse_header("=value").is_err());
    }

    #[test]
    fn message_headers_follow_argument_order() {
        let headers = make_headers(&[
            ("b".to_string(), "2".to_string()),
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "3".to_string()),
        ]);
        let pairs: Vec<(&str, &[u8])> = headers.iter().map(|h| (h.key, h.value.unwrap())).collect();
        assert_eq!(
            pairs,
            vec![
                ("b", b"2".as_slice()),
                ("a", b"1".as_slice()),
                ("b", b"3".as_slice()),
            ]
        );
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
