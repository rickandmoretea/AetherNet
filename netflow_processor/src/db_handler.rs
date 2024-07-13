use std::net::IpAddr;
use tokio_postgres::{NoTls};
use anyhow::Result;
use chrono::{DateTime, Utc};

#[derive(serde::Deserialize, serde::Serialize)]
pub struct FlowData {
    #[serde(with = "chrono::serde::ts_seconds")]
    pub(crate) time: DateTime<Utc>,
    pub(crate) src_ip: IpAddr,
    pub(crate) dst_ip: IpAddr,
    pub(crate) src_country: String,
    pub(crate) dst_country: String,
    pub(crate) src_as: i32,
    pub(crate) dst_as: i32,
    pub(crate) bytes: i64,
    pub(crate) packets: i64,
}


pub async fn connect_to_db() -> Result<tokio_postgres::Client> {
    let (client, connection) =
        tokio_postgres::connect("host=timescaledb user=postgres password=password dbname=network_traffic", NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Error connecting to database: {}", e);
        }
    });

    Ok(client)
}

pub async fn insert_flow_data(
    client: &tokio_postgres::Client,
    flow_data: &FlowData,
) -> Result<(), tokio_postgres::Error> {
    client.execute(
        "INSERT INTO flow_data (time, src_ip, dst_ip, src_country, dst_country, src_as, dst_as, bytes, packets)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        &[&flow_data.time, &flow_data.src_ip.to_string(), &flow_data.dst_ip.to_string(),
            &flow_data.src_country, &flow_data.dst_country, &flow_data.src_as, &flow_data.dst_as,
            &flow_data.bytes, &flow_data.packets],
    ).await?;

    Ok(())
}