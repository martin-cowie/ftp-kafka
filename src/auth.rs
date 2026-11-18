use async_trait::async_trait;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, Producer};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use unftp_core::auth::{
    AuthenticationError, Authenticator, Credentials, Principal, UserDetail, UserDetailError,
    UserDetailProvider,
};

use crate::kafka::KafkaConfig;

/// The Kafka producer a login establishes and its session keeps using.
pub type SessionProducer = FutureProducer<AuthContext>;

/// What librdkafka has told the client about its broker connection.
#[derive(Debug, Default)]
struct BrokerEvents {
    rejected: AtomicBool,
    reasons: Mutex<Vec<String>>,
}

/// Records librdkafka's connection errors for the login probe.
///
/// librdkafka reports a rejected SASL login, and the reason a broker cannot be
/// reached, only through this callback; a metadata request against such a
/// broker just times out.
#[derive(Debug, Default)]
pub struct AuthContext {
    events: Arc<BrokerEvents>,
}

impl ClientContext for AuthContext {
    fn error(&self, error: KafkaError, reason: &str) {
        if let KafkaError::Global(RDKafkaErrorCode::Authentication) = error {
            self.events.rejected.store(true, Ordering::SeqCst);
        }
        let mut reasons = self.events.reasons.lock().unwrap();
        if reasons.len() < MAX_REASONS && !reasons.iter().any(|known| known == reason) {
            reasons.push(reason.to_string());
        }
    }
}

/// Why a login could not reach a decision in Kafka's favour.
enum ConnectError {
    Rejected,
    Unreachable(String),
}

const MAX_REASONS: usize = 5;
const PROBE_SLICE: Duration = Duration::from_millis(100);

/// Authenticates FTP logins by connecting to Kafka with the client's credentials.
///
/// A login is accepted only if the Kafka cluster accepts the connection. The
/// producer that proved it is queued under the username until
/// `provide_user_detail` claims it for the session.
pub struct KafkaAuthenticator {
    config: KafkaConfig,
    handoff: Handoff,
    logger: slog::Logger,
}

type Handoff = Arc<Mutex<HashMap<String, Vec<SessionProducer>>>>;

/// A logged-in FTP user together with the Kafka producer their login established.
///
/// libunftp keeps one `KafkaUser` for the life of a Session, so it is also where
/// per-Session state, such as the Session topic, lives.
pub struct KafkaUser {
    pub username: String,
    pub producer: SessionProducer,
    topic: Mutex<String>,
}

impl KafkaUser {
    /// Creates a Session's user whose Session topic starts as `topic`.
    pub fn new(username: String, producer: SessionProducer, topic: String) -> Self {
        KafkaUser {
            username,
            producer,
            topic: Mutex::new(topic),
        }
    }

    /// The topic `SITE SEND` publishes to when it has no `--topic`.
    pub fn topic(&self) -> String {
        self.topic.lock().unwrap().clone()
    }

    /// Replaces the Session topic.
    pub fn set_topic(&self, topic: String) {
        *self.topic.lock().unwrap() = topic;
    }
}

impl std::fmt::Debug for KafkaUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaUser")
            .field("username", &self.username)
            .finish()
    }
}

impl std::fmt::Display for KafkaUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.username)
    }
}

impl UserDetail for KafkaUser {}

/// Claims the producer a login queued, turning a `Principal` into a `KafkaUser`.
///
/// Any queued producer for the username will do: Kafka gives one username one
/// password, so they are interchangeable.
pub struct KafkaUserDetailProvider {
    handoff: Handoff,
    default_topic: String,
}

impl std::fmt::Debug for KafkaUserDetailProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaUserDetailProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl UserDetailProvider for KafkaUserDetailProvider {
    type User = KafkaUser;

    async fn provide_user_detail(
        &self,
        principal: &Principal,
    ) -> Result<KafkaUser, UserDetailError> {
        let producer = self
            .handoff
            .lock()
            .unwrap()
            .get_mut(&principal.username)
            .and_then(Vec::pop)
            .ok_or_else(|| {
                UserDetailError::Generic(format!("no login is waiting for {}", principal.username))
            })?;
        Ok(KafkaUser::new(
            principal.username.clone(),
            producer,
            self.default_topic.clone(),
        ))
    }
}

impl std::fmt::Debug for KafkaAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaAuthenticator")
            .field("config", &self.config)
            .finish()
    }
}

impl KafkaAuthenticator {
    pub fn new(config: KafkaConfig, logger: slog::Logger) -> Self {
        KafkaAuthenticator {
            config,
            handoff: Arc::new(Mutex::new(HashMap::new())),
            logger,
        }
    }

    /// Returns the provider that hands this authenticator's logins to their sessions.
    pub fn user_detail_provider(&self) -> KafkaUserDetailProvider {
        KafkaUserDetailProvider {
            handoff: Arc::clone(&self.handoff),
            default_topic: self.config.default_topic.clone(),
        }
    }

    fn connect(
        config: &KafkaConfig,
        logger: &slog::Logger,
        username: &str,
        password: &str,
    ) -> Result<SessionProducer, ConnectError> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", config.message_timeout_ms.to_string())
            .set(
                "security.protocol",
                config.security_protocol.as_librdkafka(),
            );
        if let Some(mechanism) = config.sasl_mechanism {
            client_config
                .set("sasl.mechanism", mechanism.as_librdkafka())
                .set("sasl.username", username)
                .set("sasl.password", password);
        }
        let context = AuthContext::default();
        let events = Arc::clone(&context.events);
        let producer: SessionProducer = client_config
            .create_with_context(context)
            .map_err(|e| ConnectError::Unreachable(e.to_string()))?;

        let deadline = Instant::now() + Duration::from_millis(config.auth_timeout_ms);
        let mut last_logged = String::new();
        loop {
            match producer.client().fetch_metadata(None, PROBE_SLICE) {
                Ok(_) => return Ok(producer),
                Err(_) if events.rejected.load(Ordering::SeqCst) => {
                    return Err(ConnectError::Rejected)
                }
                Err(e) if Instant::now() >= deadline => {
                    let reasons = events.reasons.lock().unwrap();
                    let reason = if reasons.is_empty() {
                        e.to_string()
                    } else {
                        reasons.join("; ")
                    };
                    return Err(ConnectError::Unreachable(reason));
                }
                Err(e) => {
                    let error = e.to_string();
                    if error != last_logged {
                        slog::debug!(
                            logger,
                            "Kafka probe for {} failed, retrying: {}",
                            username,
                            error
                        );
                        last_logged = error;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Authenticator for KafkaAuthenticator {
    async fn authenticate(
        &self,
        username: &str,
        creds: &Credentials,
    ) -> Result<Principal, AuthenticationError> {
        let config = self.config.clone();
        let logger = self.logger.clone();
        let username_owned = username.to_string();
        let password = creds.password.clone().unwrap_or_default();
        let producer = tokio::task::spawn_blocking(move || {
            Self::connect(&config, &logger, &username_owned, &password)
        })
        .await
        .map_err(|e| AuthenticationError::new(format!("login task failed: {e}")))?
        .map_err(|e| match e {
            ConnectError::Rejected => AuthenticationError::BadPassword,
            ConnectError::Unreachable(reason) => {
                AuthenticationError::new(format!("Kafka is unreachable: {reason}"))
            }
        })?;
        self.handoff
            .lock()
            .unwrap()
            .entry(username.to_string())
            .or_default()
            .push(producer);
        Ok(Principal {
            username: username.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::{KafkaConfig, SaslMechanism, SecurityProtocol};
    use rdkafka::mocking::MockCluster;
    use unftp_core::auth::{Authenticator, Credentials, UserDetailProvider};

    fn discard_logger() -> slog::Logger {
        slog::Logger::root(slog::Discard, slog::o!())
    }

    fn config(brokers: &str) -> KafkaConfig {
        KafkaConfig {
            brokers: brokers.to_string(),
            default_topic: "test-topic".to_string(),
            message_timeout_ms: 5000,
            auth_timeout_ms: 2000,
            security_protocol: SecurityProtocol::Plaintext,
            sasl_mechanism: None,
        }
    }

    #[tokio::test]
    async fn plaintext_login_is_accepted_for_any_credentials_when_the_broker_is_reachable() {
        let cluster = MockCluster::new(1).unwrap();
        let authenticator =
            KafkaAuthenticator::new(config(&cluster.bootstrap_servers()), discard_logger());

        let principal = authenticator
            .authenticate("alice", &Credentials::from("anything"))
            .await
            .unwrap();

        assert_eq!(principal.username, "alice");
    }

    #[tokio::test]
    async fn login_is_rejected_when_the_broker_is_unreachable() {
        let mut unreachable = config("127.0.0.1:1");
        unreachable.auth_timeout_ms = 500;
        let authenticator = KafkaAuthenticator::new(unreachable, discard_logger());

        let result = authenticator
            .authenticate("alice", &Credentials::from("anything"))
            .await;

        let Err(AuthenticationError::ImplPropagated(message, _)) = result else {
            panic!("expected a Kafka-unreachable error, got {result:?}");
        };
        assert!(message.contains("Connection refused"), "{message}");
    }

    #[tokio::test]
    async fn provider_hands_the_logged_in_sessions_producer_to_it() {
        let cluster = MockCluster::new(1).unwrap();
        let authenticator =
            KafkaAuthenticator::new(config(&cluster.bootstrap_servers()), discard_logger());
        let provider = authenticator.user_detail_provider();
        let principal = authenticator
            .authenticate("alice", &Credentials::from("x"))
            .await
            .unwrap();

        let user = provider.provide_user_detail(&principal).await.unwrap();

        assert_eq!(user.username, "alice");
        assert_eq!(user.topic(), "test-topic");
        let metadata = user
            .producer
            .client()
            .fetch_metadata(None, Duration::from_secs(2));
        assert!(metadata.is_ok());
    }

    #[tokio::test]
    async fn provider_fails_when_no_login_is_waiting_for_the_user() {
        let authenticator = KafkaAuthenticator::new(config("127.0.0.1:1"), discard_logger());
        let provider = authenticator.user_detail_provider();

        let result = provider
            .provide_user_detail(&Principal {
                username: "alice".to_string(),
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn sasl_login_is_rejected_by_a_broker_that_does_not_speak_sasl() {
        for mechanism in [
            SaslMechanism::Plain,
            SaslMechanism::ScramSha256,
            SaslMechanism::ScramSha512,
        ] {
            let cluster = MockCluster::new(1).unwrap();
            let mut sasl = config(&cluster.bootstrap_servers());
            sasl.security_protocol = SecurityProtocol::SaslPlaintext;
            sasl.sasl_mechanism = Some(mechanism);
            sasl.auth_timeout_ms = 1000;
            let authenticator = KafkaAuthenticator::new(sasl, discard_logger());

            let result = authenticator
                .authenticate("alice", &Credentials::from("secret"))
                .await;

            assert!(
                matches!(result, Err(AuthenticationError::BadPassword)),
                "{mechanism:?}"
            );
        }
    }
}
