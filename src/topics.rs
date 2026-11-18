use async_trait::async_trait;
use libunftp::options::{Reply, ReplyCode, SiteCommandContext, SiteCommandHandler};
use rdkafka::producer::Producer;
use std::time::Duration;

use crate::auth::{KafkaUser, SessionProducer};
use crate::kafka::logged_in_user;
use crate::storage::MemStorage;

const METADATA_TIMEOUT: Duration = Duration::from_secs(5);
const INTERNAL_TOPIC_PREFIX: &str = "__";

/// Lists the topics on the cluster that a client may publish to, sorted by name.
///
/// Queries the cluster with the Session's own Kafka credentials. Internal
/// topics, such as `__consumer_offsets`, are left out.
///
/// # Errors
/// Returns a description of the failure when the cluster cannot be queried.
async fn cluster_topics(producer: &SessionProducer) -> Result<Vec<String>, String> {
    let producer = producer.clone();
    let metadata = tokio::task::spawn_blocking(move || {
        producer.client().fetch_metadata(None, METADATA_TIMEOUT)
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e.to_string())?;

    let mut result: Vec<String> = metadata
        .topics()
        .iter()
        .map(|topic| topic.name().to_string())
        .filter(|name| !name.starts_with(INTERNAL_TOPIC_PREFIX))
        .collect();
    result.sort();
    Ok(result)
}

/// Lists the cluster's topics: `SITE TOPICS`.
#[derive(Debug)]
pub struct TopicsHandler;

#[async_trait]
impl SiteCommandHandler<MemStorage, KafkaUser> for TopicsHandler {
    async fn handle(&self, context: &SiteCommandContext<MemStorage, KafkaUser>) -> Reply {
        let user = match logged_in_user(context) {
            Ok(user) => user,
            Err(reply) => return reply,
        };
        match cluster_topics(&user.producer).await {
            Ok(topics) if topics.is_empty() => {
                Reply::new(ReplyCode::CommandOkay, "No topics on the cluster")
            }
            Ok(topics) => Reply::new_multiline(ReplyCode::CommandOkay, topics),
            Err(e) => Reply::new(ReplyCode::LocalError, &format!("Cannot list topics: {e}")),
        }
    }
}

/// Reports or sets the Session topic: `SITE TOPIC [<topic>]`.
#[derive(Debug)]
pub struct TopicHandler;

#[async_trait]
impl SiteCommandHandler<MemStorage, KafkaUser> for TopicHandler {
    async fn handle(&self, context: &SiteCommandContext<MemStorage, KafkaUser>) -> Reply {
        let user = match logged_in_user(context) {
            Ok(user) => user,
            Err(reply) => return reply,
        };
        let arguments: Vec<&str> = context.arguments.split_whitespace().collect();
        match arguments.as_slice() {
            [] => Reply::new(
                ReplyCode::CommandOkay,
                &format!("Current topic: {}", user.session_topic()),
            ),
            [topic] => select_topic(user, topic).await,
            _ => Reply::new(
                ReplyCode::ParameterSyntaxError,
                "Expected at most one topic name",
            ),
        }
    }
}

/// Makes `topic` the Session topic if it exists on the cluster.
///
/// The cluster's topic list is checked, rather than asking for `topic`'s
/// metadata, because a metadata request can make a broker auto-create the topic.
async fn select_topic(user: &KafkaUser, topic: &str) -> Reply {
    match cluster_topics(&user.producer).await {
        Ok(topics) if topics.iter().any(|known| known == topic) => {
            user.select_topic(topic);
            Reply::new(ReplyCode::CommandOkay, &format!("Current topic: {topic}"))
        }
        Ok(_) => Reply::new(
            ReplyCode::FileError,
            &format!("Topic \"{topic}\" does not exist"),
        ),
        Err(e) => Reply::new(ReplyCode::LocalError, &format!("Cannot check topic: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthContext;
    use rdkafka::config::ClientConfig;
    use rdkafka::mocking::MockCluster;
    use std::sync::Arc;

    fn user_of(brokers: &str) -> KafkaUser {
        let producer: SessionProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create_with_context(AuthContext::default())
            .unwrap();
        KafkaUser::new("alice".to_string(), producer, "default-topic".to_string())
    }

    fn context(
        arguments: &str,
        user: Option<KafkaUser>,
    ) -> SiteCommandContext<MemStorage, KafkaUser> {
        SiteCommandContext {
            command: "TOPIC".to_string(),
            arguments: arguments.to_string(),
            username: user.as_ref().map(|_| "alice".to_string()),
            storage: Arc::new(MemStorage::new()),
            user: Arc::new(user),
            logger: slog::Logger::root(slog::Discard, slog::o!()),
        }
    }

    fn is_code(reply: &Reply, expected: ReplyCode) -> bool {
        matches!(reply, Reply::CodeAndMsg { code, .. } if *code == expected)
            || matches!(reply, Reply::MultiLine { code, .. } if *code == expected)
    }

    #[tokio::test]
    async fn both_commands_refuse_a_client_that_is_not_logged_in() {
        let ctx = context("", None);
        assert!(is_code(
            &TopicsHandler.handle(&ctx).await,
            ReplyCode::NotLoggedIn
        ));
        assert!(is_code(
            &TopicHandler.handle(&ctx).await,
            ReplyCode::NotLoggedIn
        ));
    }

    #[tokio::test]
    async fn topic_without_an_argument_reports_the_session_topic() {
        let ctx = context("", Some(user_of("127.0.0.1:1")));
        let reply = TopicHandler.handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::CommandOkay));
        assert!(
            matches!(&reply, Reply::CodeAndMsg { msg, .. } if msg.contains("default-topic")),
            "{reply:?}"
        );
    }

    #[tokio::test]
    async fn topic_rejects_more_than_one_argument() {
        let ctx = context("a b", Some(user_of("127.0.0.1:1")));
        let reply = TopicHandler.handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::ParameterSyntaxError));
    }

    #[tokio::test]
    async fn topic_is_refused_and_unchanged_when_it_is_not_on_the_cluster() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("known", 1, 1).unwrap();
        let user = user_of(&cluster.bootstrap_servers());
        let ctx = context("unknown", Some(user));

        let reply = TopicHandler.handle(&ctx).await;

        assert!(is_code(&reply, ReplyCode::FileError));
        let session = ctx.user.as_ref().as_ref().unwrap();
        assert_eq!(session.session_topic(), "default-topic");
    }

    #[tokio::test]
    async fn topic_is_selected_when_it_is_on_the_cluster() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("known", 1, 1).unwrap();
        let ctx = context("known", Some(user_of(&cluster.bootstrap_servers())));

        let reply = TopicHandler.handle(&ctx).await;

        assert!(is_code(&reply, ReplyCode::CommandOkay));
        let session = ctx.user.as_ref().as_ref().unwrap();
        assert_eq!(session.session_topic(), "known");
    }

    #[tokio::test]
    async fn topics_lists_the_clusters_topics_sorted_without_internal_ones() {
        let cluster = MockCluster::new(1).unwrap();
        for name in ["beta", "alpha", "__consumer_offsets"] {
            cluster.create_topic(name, 1, 1).unwrap();
        }
        let ctx = context("", Some(user_of(&cluster.bootstrap_servers())));

        let reply = TopicsHandler.handle(&ctx).await;

        let Reply::MultiLine { code, lines } = reply else {
            panic!("expected a multi-line reply, got {reply:?}");
        };
        assert_eq!(code, ReplyCode::CommandOkay);
        assert_eq!(lines, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn topics_fails_when_the_cluster_is_unreachable() {
        let ctx = context("", Some(user_of("127.0.0.1:1")));
        let reply = TopicsHandler.handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::LocalError));
    }
}
