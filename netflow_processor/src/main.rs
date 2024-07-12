use std::net::IpAddr;
use std::sync::Arc;
use anyhow::Result;
use netflow_parser::{NetflowParser, NetflowPacketResult};
use netflow_parser::variable_versions::common::{DataNumber, FieldValue};
use netflow_parser::variable_versions::ipfix_lookup::IPFixField;
use netflow_parser::variable_versions::v9::FlowSetBody;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio_postgres::{NoTls, Error as PgError};
use chrono::{DateTime, Utc};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::{Message, OwnedMessage};
use serde::{Serialize, Deserialize};
use crate::enricher::DataEnricher;

mod enricher;

#[derive(Serialize, Deserialize)]
struct FlowData {
    time: DateTime<Utc>,
    src_ip: IpAddr,
    dst_ip: IpAddr,
    src_country: String,
    dst_country: String,
    src_as: i32,
    dst_as: i32,
    bytes: i64,
    packets: i64,
}

async fn insert_flow_data(
    client: &tokio_postgres::Client,
    flow_data: &FlowData,
) -> Result<(), PgError> {
    client.execute(
        "INSERT INTO flow_data (time, src_ip, dst_ip, src_country, dst_country, src_as, dst_as, bytes, packets)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        &[&flow_data.time, &flow_data.src_ip.to_string(), &flow_data.dst_ip.to_string(),
            &flow_data.src_country, &flow_data.dst_country, &flow_data.src_as, &flow_data.dst_as,
            &flow_data.bytes, &flow_data.packets],
    ).await?;

    Ok(())
}

async fn send_to_kafka(producer: &FutureProducer, topic: &str, key: &str, value: &str) -> std::result::Result<(), (KafkaError, OwnedMessage)> {
    producer.send(
        FutureRecord::to(topic)
            .key(key)
            .payload(value),
        std::time::Duration::from_secs(0),
    ).await.map(|_| ())
}

async fn process_v5_packet(v5: netflow_parser::V5, enricher: &DataEnricher, producer: &FutureProducer) -> Result<(), Box<dyn std::error::Error>> {
    for flowset in v5.flowsets {
        let src_ip = IpAddr::V4(flowset.src_addr);
        let dst_ip = IpAddr::V4(flowset.dst_addr);

        let (src_country, src_as) = enricher.enrich(src_ip);
        let (dst_country, dst_as) = enricher.enrich(dst_ip);

        let flow_data = FlowData {
            time: Utc::now(),
            src_ip,
            dst_ip,
            src_country: src_country.unwrap_or_default(),
            dst_country: dst_country.unwrap_or_default(),
            src_as: src_as.unwrap_or_default() as i32,
            dst_as: dst_as.unwrap_or_default() as i32,
            bytes: flowset.d_octets as i64,
            packets: flowset.d_pkts as i64,
        };

        let kafka_value = serde_json::to_string(&flow_data)?;
        send_to_kafka(producer, "netflow_data", &format!("{}-{}", src_ip, dst_ip), &kafka_value).await?;
    }
    Ok(())
}

async fn process_ipfix_packet(ipfix: netflow_parser::IPFix, enricher: &DataEnricher, producer: &FutureProducer) -> Result<(), Box<dyn std::error::Error>> {
    for flowset in &ipfix.flowsets {
        if let FlowSetBody { data: Some(data), .. } = &flowset.body {
            for record in &data.data_fields {
                let src_ip = record.get(&(IPFixField::SourceIpv4address as usize))
                    .and_then(|(_, value)| if let FieldValue::Ip4Addr(ip) = value { Some(IpAddr::V4(*ip)) } else { None });
                let dst_ip = record.get(&(IPFixField::DestinationIpv4address as usize))
                    .and_then(|(_, value)| if let FieldValue::Ip4Addr(ip) = value { Some(IpAddr::V4(*ip)) } else { None });

                if let (Some(src_ip), Some(dst_ip)) = (src_ip, dst_ip) {
                    let (src_country, src_as) = enricher.enrich(src_ip);
                    let (dst_country, dst_as) = enricher.enrich(dst_ip);

                    let bytes = record.get(&(IPFixField::OctetDeltaCount as usize))
                        .and_then(|(_, value)| if let FieldValue::DataNumber(DataNumber::U64(b)) = value { Some(*b as i64) } else { None })
                        .unwrap_or(0);
                    let packets = record.get(&(IPFixField::PacketDeltaCount as usize))
                        .and_then(|(_, value)| if let FieldValue::DataNumber(DataNumber::U64(p)) = value { Some(*p as i64) } else { None })
                        .unwrap_or(0);

                    let flow_data = FlowData {
                        time: Utc::now(),
                        src_ip,
                        dst_ip,
                        src_country: src_country.unwrap_or_default(),
                        dst_country: dst_country.unwrap_or_default(),
                        src_as: src_as.unwrap_or_default() as i32,
                        dst_as: dst_as.unwrap_or_default() as i32,
                        bytes,
                        packets,
                    };

                    let kafka_value = serde_json::to_string(&flow_data)?;
                    send_to_kafka(producer, "netflow_data", &format!("{}-{}", src_ip, dst_ip), &kafka_value).await?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:2055";
    let socket = UdpSocket::bind(addr).await?;
    println!("Listening on: {}", addr);

    let parser = Arc::new(Mutex::new(NetflowParser::default()));
    let enricher = Arc::new(DataEnricher::new()?);
    let mut buf = [0u8; 1500];

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "kafka:29092")
        .set("message.timeout.ms", "5000")
        .create()?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "netflow_consumer")
        .set("bootstrap.servers", "kafka:29092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()?;

    consumer.subscribe(&["netflow_data"])?;

    let (db_client, connection) =
        tokio_postgres::connect("host=timescaledb user=postgres password=password dbname=network_traffic", NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Spawn a task for the Kafka consumer
    let db_client_clone = db_client.clone();
    tokio::spawn(async move {
        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    let payload = msg.payload().unwrap();
                    let flow_data: FlowData = serde_json::from_slice(payload).unwrap();

                    if let Err(e) = insert_flow_data(&db_client_clone, &flow_data).await {
                        eprintln!("Error inserting flow data: {}", e);
                    }
                }
                Err(e) => eprintln!("Kafka error: {}", e),
            }
        }
    });

    // Main loop for receiving and processing Netflow packets
    loop {
        let (len, _) = socket.recv_from(&mut buf).await?;
        let parser_clone = Arc::clone(&parser);
        let enricher_clone = Arc::clone(&enricher);
        let producer_clone = producer.clone();

        tokio::spawn(async move {
            let mut parser = parser_clone.lock().await;
            let parsed_packets = parser.parse_bytes(&buf[..len]);

            for packet in parsed_packets {
                match packet {
                    NetflowPacketResult::V5(v5) => {
                        if let Err(e) = process_v5_packet(v5, &enricher_clone, &producer_clone).await {
                            eprintln!("Error processing V5 packet: {}", e);
                        }
                    }
                    NetflowPacketResult::IPFix(ipfix) => {
                        if let Err(e) = process_ipfix_packet(ipfix, &enricher_clone, &producer_clone).await {
                            eprintln!("Error processing IPFIX packet: {}", e);
                        }
                    }
                    NetflowPacketResult::Error(e) => {
                        eprintln!("Error parsing packet: {:?}", e);
                    }
                    _ => {
                        eprintln!("Unsupported packet type");
                    }
                }
            }
        });
    }
}