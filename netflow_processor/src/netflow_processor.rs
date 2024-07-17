use std::net::IpAddr;
use tokio::net::UdpSocket;
use std::sync::Arc;
use tokio::sync::Mutex;
use netflow_parser::{NetflowParser, NetflowPacketResult};
use crate::enricher::DataEnricher;
use anyhow::{Context, Result};
use chrono::Utc;
use netflow_parser::static_versions::v5::V5;
use netflow_parser::variable_versions::common::{DataNumber, FieldValue};
use netflow_parser::variable_versions::ipfix::{FlowSetBody, IPFix};
use netflow_parser::variable_versions::ipfix_lookup::IPFixField;
use rdkafka::producer::FutureProducer;
use tokio_postgres::Client;
use tracing::{debug, error, info, warn};
use crate::db_handler::FlowData;
use crate::kafka_handler::send_to_kafka;

pub async fn start_processor(
    _db_client: Arc<Client>,
    producer: FutureProducer,
    enricher: DataEnricher,
) -> Result<()> {
    let addr = "0.0.0.0:2055";
    let socket = UdpSocket::bind(addr).await.context("Failed to bind to UDP socket")?;
    info!("Netflow processor listening on {}", addr);

    let parser = Arc::new(Mutex::new(NetflowParser::default()));
    let mut buf = [0u8; 1500];

    loop {
        let (len, addr) = socket.recv_from(&mut buf).await.context("Failed to receive data")?;
        debug!("Received {} bytes from {}", len, addr);

        let parser_clone = Arc::clone(&parser);
        let producer_clone = producer.clone();
        let enricher_clone = enricher.clone();

        tokio::spawn(async move {
            let mut parser = parser_clone.lock().await;
            let parsed_packets = parser.parse_bytes(&buf[..len]);

            for packet in parsed_packets {
                match packet {
                    NetflowPacketResult::V5(v5) => {
                        debug!("Processing Netflow V5 packet");
                        if let Err(e) = process_v5_packet(v5, &enricher_clone, &producer_clone).await {
                            warn!("Error processing V5 packet: {}", e);
                        }
                    }
                    NetflowPacketResult::IPFix(ipfix) => {
                        debug!("Processing IPFIX packet");
                        if let Err(e) = process_ipfix_packet(ipfix, &enricher_clone, &producer_clone).await {
                            warn!("Error processing IPFIX packet: {}", e);
                        }
                    }
                    NetflowPacketResult::Error(e) => {
                        warn!("Error parsing packet: {:?}", e);
                    }
                    _ => {
                        error!("Unsupported packet type");
                    }
                }
            }
        });
    }
}


async fn process_v5_packet(v5: V5, enricher: &DataEnricher, producer: &FutureProducer) -> Result<(), Box<dyn std::error::Error>> {
    for flowset in v5.flowsets {
        let src_ip = IpAddr::V4(flowset.src_addr);
        let dst_ip = IpAddr::V4(flowset.dst_addr);

        let (src_country, src_as) = enricher.enrich(src_ip);
        let (dst_country, dst_as) = enricher.enrich(dst_ip);

        debug!("Enriched V5 flow: src_ip={}, src_country{:?}, src_as={:?}, dst_ip={}, dst_country={:?}, dst_as={:?}",
               src_ip, src_country, src_as, dst_ip, dst_country, dst_as);

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
        info!("Sent V5 flow data to Kafka: src_ip={}, dst_ip={}", src_ip, dst_ip);
    }
    Ok(())
}

async fn process_ipfix_packet(ipfix: IPFix, enricher: &DataEnricher, producer: &FutureProducer) -> Result<(), Box<dyn std::error::Error>> {
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
                    info!("Sent IPFIX flow data to Kafka: src_ip={}, dst_ip={}", src_ip, dst_ip);
                }
            }
        }
    }
    Ok(())
}