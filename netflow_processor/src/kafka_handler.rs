use std::sync::Arc;
use std::time::Duration;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::ClientConfig;
use rdkafka::message::{Message};
use anyhow::{Context, Result};
use tokio_postgres::Client;
use crate::db_handler::{FlowData, insert_flow_data};


pub fn create_producer() -> Result<FutureProducer> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "kafka:29092")
        .set("message.timeout.ms", "5000")
        .create().context("Failed to create Kafka producer")?;
    Ok(producer)
}

pub fn create_consumer() -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "netflow_consumer")
        .set("bootstrap.servers", "kafka:29092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()?;

    consumer.subscribe(&["netflow_data"]).context("Failed to subscribe to topic")?;
    Ok(consumer)
}

pub async fn send_to_kafka(producer: &FutureProducer, topic: &str, key: &str, value: &str) -> Result<()> {
    producer.send(
        FutureRecord::to(topic)
            .key(key)
            .payload(value),
        Duration::from_secs(0),
    ).await
        .map(|_| ())
        .map_err(|(err, _)| anyhow::anyhow!("Failed to send message to Kafka: {}", err))
}


pub async fn start_consumer(consumer: StreamConsumer, db_client: Arc<Client>) {
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let payload = msg.payload().unwrap();
                let flow_data: FlowData = serde_json::from_slice(payload).unwrap();

                if let Err(e) = insert_flow_data(&db_client, &flow_data).await {
                    eprintln!("Error inserting flow data: {}", e);
                }
            }
            Err(e) => eprintln!("Kafka error: {}", e),
        }
    }
}