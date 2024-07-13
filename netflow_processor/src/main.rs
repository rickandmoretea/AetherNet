mod netflow_processor;
mod kafka_handler;
mod db_handler;
mod enricher;
mod api;

use std::sync::Arc;
use tracing_subscriber::{fmt, EnvFilter};
use tokio;
use anyhow::Result;
use tracing::info;

fn init_logger() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    init_logger();

    info!("Starting NetFlow processor");

    let db_client = Arc::new(db_handler::connect_to_db().await?);
    let kafka_producer = kafka_handler::create_producer()?;
    let kafka_consumer = kafka_handler::create_consumer()?;
    let enricher = enricher::DataEnricher::new()?;

    // Start Kafka consumer
    let consumer_db_client = Arc::clone(&db_client);
    tokio::spawn(async move {
        kafka_handler::start_consumer(kafka_consumer, consumer_db_client).await;
    });

    // Start NetFlow processor
    let netflow_db_client = Arc::clone(&db_client);
    tokio::spawn(async move {
        if let Err(e) = netflow_processor::start_processor(netflow_db_client, kafka_producer, enricher).await {
            eprintln!("NetFlow processor error: {}", e);
        }
    });

    // Run API server in the main thread
    api::run_api(Arc::clone(&db_client)).await?;
    Ok(())
}