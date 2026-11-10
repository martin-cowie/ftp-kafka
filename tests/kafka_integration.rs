//! End-to-end test of `SITE SEND` against a real Kafka broker, provided by
//! `testcontainers`. Requires a working Docker daemon, so it's excluded from
//! the default `cargo test` run - opt in explicitly with:
//!
//! ```sh
//! cargo test --test kafka_integration -- --ignored
//! ```
//!
//! Docker maps the container's Kafka listener to a random host port, which
//! the test passes to the server through `config.toml`.

mod common;

use std::time::Duration;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};

use common::{TestServer, PASSWORD, USERNAME};

const TOPIC: &str = "rust-topic";

#[tokio::test]
#[ignore = "requires a running Docker daemon"]
async fn site_send_publishes_the_uploaded_file_to_kafka() {
    let kafka_container = Kafka::default()
        .start()
        .await
        .expect("failed to start the Kafka test container");
    let kafka_port = kafka_container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("failed to read the Kafka container's host port");
    let brokers = format!("127.0.0.1:{kafka_port}");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "ftp-kafka-integration-test")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("failed to create Kafka consumer");
    consumer.subscribe(&[TOPIC]).expect("failed to subscribe to topic");

    let server = TestServer::start_with_config(&format!(
        r#"
            [kafka]
            brokers = "{brokers}"
            default_topic = "{TOPIC}"

            [[users]]
            username = "{USERNAME}"
            password = "{PASSWORD}"
        "#
    ));
    let mut client = server.connect();
    client.login(USERNAME, PASSWORD);

    let store_reply = client.store("hello.txt", b"hello kafka");
    assert!(store_reply.starts_with('2'), "STOR failed: {store_reply}");

    let send_reply = client.command("SITE SEND hello.txt");
    assert!(send_reply.starts_with('2'), "SITE SEND failed: {send_reply}");

    // `kafka.rs` relies on Kafka's auto-create-topics behaviour rather than
    // pre-creating the topic, so the consumer may see `UnknownTopicOrPartition`
    // for a brief moment while the topic is created and its metadata
    // propagates. Retry until a message arrives or the overall timeout fires.
    let message = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match consumer.recv().await {
                Ok(message) => return message,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .expect("timed out waiting for the Kafka message");

    assert_eq!(message.key(), Some("hello.txt".as_bytes()));
    assert_eq!(message.payload(), Some("hello kafka".as_bytes()));
}
