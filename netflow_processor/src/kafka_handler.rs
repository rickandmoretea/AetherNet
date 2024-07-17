// use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::ClientConfig;
use rdkafka::message::{Message};
use anyhow::{Context, Result};
// use serde::{Serialize, Serializer};
use tokio_postgres::Client;
use tracing::{error, info};
use crate::db_handler::{FlowData, insert_flow_data};
// struct MyIpAddr(IpAddr);

// impl Serialize for MyIpAddr {
//     fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         serializer.serialize_str(&self.to_string())
//     }
// }

pub fn create_producer() -> Result<FutureProducer> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create().context("Failed to create Kafka producer")?;
    info!("Created Kafka producer");
    Ok(producer)
}

pub fn create_consumer() -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "netflow_consumer")
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()?;

    consumer.subscribe(&["netflow_data"]).context("Failed to subscribe to topic")?;
    info!("Created Kafka consumer");
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
                if let Some(payload) = msg.payload() {
                    match serde_json::from_slice::<FlowData>(payload) {
                        Ok(flow_data) => {
                            if let Err(e) = insert_flow_data(&db_client, &flow_data).await {
                                error!("Error inserting flow data: {}", e);
                            }
                        },
                        Err(e) => error!("Error deserializing flow data: {}", e),
                    }
                } else {
                    error!("Received message with empty payload");
                }
            }
            Err(e) => error!("Kafka error: {}", e),
        }
    }
}