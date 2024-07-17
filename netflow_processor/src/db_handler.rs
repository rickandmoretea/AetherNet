use std::net::IpAddr;
use tokio_postgres::{Client, Error, NoTls};
use anyhow::Result;
use chrono::{DateTime, Utc};
use tracing::info;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct FlowData {
    #[serde(with = "chrono::serde::ts_seconds")]
    pub(crate) time: DateTime<Utc>,
    #[serde(with = "ip_addr_as_string")]
    pub(crate) src_ip: IpAddr,
    #[serde(with = "ip_addr_as_string")]
    pub(crate) dst_ip: IpAddr,
    pub(crate) src_country: String,
    pub(crate) dst_country: String,
    pub(crate) src_as: i32,
    pub(crate) dst_as: i32,
    pub(crate) bytes: i64,
    pub(crate) packets: i64,
}

mod ip_addr_as_string {
    use std::net::IpAddr;
    use serde::{self, Deserialize, Serializer, Deserializer};

    pub fn serialize<S>(ip: &IpAddr, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&ip.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<IpAddr, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

pub async fn connect_to_db() -> Result<Client, Error> {
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=postgres password=password dbname=network_traffic", NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    info!("Connected to database");
    Ok(client)
}


pub async fn insert_flow_data(
    client: &Client,
    flow_data: &FlowData,
) -> Result<(), Error> {
    client.execute(
        "INSERT INTO flow_data (time, src_ip, dst_ip, src_country, dst_country, src_as, dst_as, bytes, packets)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        &[
            &flow_data.time,
            &flow_data.src_ip,
            &flow_data.dst_ip,
            &flow_data.src_country,
            &flow_data.dst_country,
            &flow_data.src_as,
            &flow_data.dst_as,
            &flow_data.bytes,
            &flow_data.packets,
        ],
    ).await?;

    Ok(())
}