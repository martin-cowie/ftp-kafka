//! End-to-end test of `SITE SEND` against a real Kafka broker, provided by
//! `testcontainers`. Requires a working Docker daemon, so it's excluded from
//! the default `cargo test` run - opt in explicitly with:
//!
//! ```sh
//! cargo test --test kafka_integration -- --ignored
//! ```
//!
//! `kafka.rs` currently hardcodes the broker address as `localhost:9092`
//! (see issue: "Kafka broker address is hardcoded"), so this test pins the
//! container's Kafka listener to that exact host port rather than letting
//! Docker assign a random one. Once the broker address is made
//! configurable, this can be relaxed and the test made safe to run
//! concurrently with others.

mod common;

use std::time::Duration;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers_modules::kafka::{Kafka, KAFKA_PORT};

use common::{TestServer, PASSWORD, USERNAME};

const TOPIC: &str = "rust-topic";

#[tokio::test]
#[ignore = "requires a running Docker daemon"]
async fn site_send_publishes_the_uploaded_file_to_kafka() {
    let _kafka_container = Kafka::default()
        .with_mapped_port(9092, KAFKA_PORT)
        .start()
        .await
        .expect("failed to start the Kafka test container");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "ftp-kafka-integration-test")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("failed to create Kafka consumer");
    consumer.subscribe(&[TOPIC]).expect("failed to subscribe to topic");

    let server = TestServer::start();
    let mut client = server.connect();
    client.login(USERNAME, PASSWORD);

    let store_reply = client.store("hello.txt", b"hello kafka");
    assert!(store_reply.starts_with('2'), "STOR failed: {store_reply}");

    let send_reply = client.command("SITE SEND hello.txt");
    assert!(send_reply.starts_with('2'), "SITE SEND failed: {send_reply}");

    let message = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
        .await
        .expect("timed out waiting for the Kafka message")
        .expect("error receiving from Kafka");

    assert_eq!(message.key(), Some("hello.txt".as_bytes()));
    assert_eq!(message.payload(), Some("hello kafka".as_bytes()));
}
