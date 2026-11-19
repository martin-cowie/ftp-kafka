//! End-to-end tests of `SITE SEND` against librdkafka's in-process mock
//! cluster. Unlike `kafka_integration.rs`, these need no Docker, so they run
//! with every `cargo test`. The mock implements only part of the Kafka
//! protocol; `kafka_integration.rs` remains the check against a real broker.

mod common;

use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message, OwnedMessage};
use rdkafka::mocking::MockCluster;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

use common::{FtpClient, TestServer, PASSWORD, USERNAME};

const TOPIC: &str = "mock-topic";

#[test]
fn site_send_publishes_the_file_with_its_name_as_key_and_no_headers() {
    let kafka = MockKafka::start();
    let server = kafka.ftp_server();
    let mut client = logged_in_client(&server);
    client.store("hello.txt", b"hello kafka");

    let reply = client.command("SITE SEND hello.txt");
    assert!(reply.starts_with('2'), "SITE SEND failed: {reply}");

    let message = kafka.consume_one();
    assert_eq!(message.key(), Some("hello.txt".as_bytes()));
    assert_eq!(message.payload(), Some("hello kafka".as_bytes()));
    assert_eq!(header_pairs(&message), Vec::<(String, String)>::new());
}

#[test]
fn site_send_attaches_headers_in_the_order_given() {
    let kafka = MockKafka::start();
    let server = kafka.ftp_server();
    let mut client = logged_in_client(&server);
    client.store("a.json", b"{}");

    let reply = client.command(
        "SITE SEND --header content-type=application/json --header \"source=ftp gateway\" \
         --header content-type=text/plain a.json",
    );
    assert!(reply.starts_with('2'), "SITE SEND failed: {reply}");

    let message = kafka.consume_one();
    assert_eq!(
        header_pairs(&message),
        vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("source".to_string(), "ftp gateway".to_string()),
            ("content-type".to_string(), "text/plain".to_string()),
        ]
    );
}

#[test]
fn site_topic_reports_the_default_topic_on_a_fresh_session() {
    let kafka = MockKafka::start();
    let server = kafka.ftp_server();
    let mut client = logged_in_client(&server);

    let reply = client.command("SITE TOPIC");

    assert!(reply.starts_with("200"), "SITE TOPIC failed: {reply}");
    assert!(reply.contains(TOPIC), "{reply}");
}

#[test]
fn site_topics_lists_the_clusters_topics() {
    let kafka = MockKafka::start();
    kafka.create_topic("another-topic");
    let server = kafka.ftp_server();
    let mut client = logged_in_client(&server);

    let reply = client.command("SITE TOPICS");

    assert!(reply.starts_with("200"), "SITE TOPICS failed: {reply}");
    assert!(reply.contains("another-topic"), "{reply}");
    assert!(reply.contains(TOPIC), "{reply}");
}

#[test]
fn site_send_uses_the_session_topic_but_the_topic_option_still_wins() {
    let kafka = MockKafka::start();
    kafka.create_topic("session-topic");
    kafka.create_topic("option-topic");
    let server = kafka.ftp_server();
    let mut client = logged_in_client(&server);
    client.store("a.txt", b"from the session topic");
    client.store("b.txt", b"from the option");

    let select = client.command("SITE TOPIC session-topic");
    assert!(select.starts_with("200"), "SITE TOPIC failed: {select}");
    let by_session = client.command("SITE SEND a.txt");
    assert!(
        by_session.starts_with("200"),
        "SITE SEND failed: {by_session}"
    );
    let by_option = client.command("SITE SEND --topic option-topic b.txt");
    assert!(
        by_option.starts_with("200"),
        "SITE SEND failed: {by_option}"
    );

    assert_eq!(
        kafka.consume_one_from("session-topic").payload(),
        Some("from the session topic".as_bytes())
    );
    assert_eq!(
        kafka.consume_one_from("option-topic").payload(),
        Some("from the option".as_bytes())
    );
}

#[test]
fn site_topic_refuses_a_topic_that_does_not_exist_and_keeps_the_current_one() {
    let kafka = MockKafka::start();
    let server = kafka.ftp_server();
    let mut client = logged_in_client(&server);

    let refused = client.command("SITE TOPIC no-such-topic");
    let current = client.command("SITE TOPIC");

    assert!(refused.starts_with('5'), "expected a 5xx reply: {refused}");
    assert!(current.contains(TOPIC), "{current}");
}

#[test]
fn the_session_topic_does_not_leak_into_other_sessions() {
    let kafka = MockKafka::start();
    kafka.create_topic("session-topic");
    let server = kafka.ftp_server();
    let mut first = logged_in_client(&server);
    let mut second = logged_in_client(&server);

    first.command("SITE TOPIC session-topic");
    let reply = second.command("SITE TOPIC");

    assert!(reply.contains(TOPIC), "{reply}");
}

/// A single-broker mock cluster with one single-partition topic, `TOPIC`.
struct MockKafka {
    cluster: MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
}

impl MockKafka {
    fn start() -> Self {
        let cluster = MockCluster::new(1).expect("failed to start the mock Kafka cluster");
        cluster
            .create_topic(TOPIC, 1, 1)
            .expect("failed to create the mock topic");
        MockKafka { cluster }
    }

    fn create_topic(&self, name: &str) {
        self.cluster
            .create_topic(name, 1, 1)
            .expect("failed to create the mock topic");
    }

    /// Starts an FTP server whose `config.toml` points at this cluster.
    fn ftp_server(&self) -> TestServer {
        TestServer::start_with_config(&format!(
            r#"
                [kafka]
                brokers = "{brokers}"
                default_topic = "{TOPIC}"
            "#,
            brokers = self.cluster.bootstrap_servers(),
        ))
    }

    /// Reads the first message on `TOPIC`, failing the test after ten seconds.
    fn consume_one(&self) -> OwnedMessage {
        self.consume_one_from(TOPIC)
    }

    /// Reads the first message on `topic`, failing the test after ten seconds.
    fn consume_one_from(&self, topic: &str) -> OwnedMessage {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.cluster.bootstrap_servers())
            .set("group.id", "ftp-kafka-mock-test")
            .create()
            .expect("failed to create Kafka consumer");
        let mut partitions = TopicPartitionList::new();
        partitions
            .add_partition_offset(topic, 0, Offset::Beginning)
            .unwrap();
        consumer.assign(&partitions).unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Some(Ok(message)) = consumer.poll(Duration::from_millis(100)) {
                return message.detach();
            }
        }
        panic!("timed out waiting for a message on {topic}");
    }
}

fn logged_in_client(server: &TestServer) -> FtpClient {
    let mut result = server.connect();
    result.login(USERNAME, PASSWORD);
    result
}

/// Returns a message's headers as UTF-8 name/value pairs, in order.
fn header_pairs(message: &OwnedMessage) -> Vec<(String, String)> {
    message
        .headers()
        .map(|headers| {
            headers
                .iter()
                .map(|header| {
                    let value = header
                        .value
                        .map(String::from_utf8_lossy)
                        .unwrap_or_default();
                    (header.key.to_string(), value.into_owned())
                })
                .collect()
        })
        .unwrap_or_default()
}
