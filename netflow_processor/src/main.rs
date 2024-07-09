mod enricher;

use std::net::IpAddr;
use std::sync::Arc;
use anyhow::Result;
use netflow_parser::{NetflowParser, NetflowPacketResult};
use netflow_parser::variable_versions::common::{DataNumber, FieldValue};
use netflow_parser::variable_versions::ipfix_lookup::IPFixField;
use netflow_parser::variable_versions::v9::FlowSetBody;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use crate::enricher::DataEnricher;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:2055";
    let socket = UdpSocket::bind(addr).await?;
    println!("Listening on: {}", addr);

    let parser = Arc::new(Mutex::new(NetflowParser::default()));
    let enricher = Arc::new(DataEnricher::new()?);
    let mut buf = [0u8; 1500]; // 1500 is the maximum size of an Ethernet frame

    loop {
        let (len, _) = socket.recv_from(&mut buf).await?;
        let parser_clone = Arc::clone(&parser);
        let enricher_clone = Arc::clone(&enricher);
        let packet_data = buf[..len].to_vec();

        tokio::spawn(async move {
            let mut parser = parser_clone.lock().await;
            let parsed_packets = parser.parse_bytes(&packet_data);

            for packet in parsed_packets {
                match packet {
                    NetflowPacketResult::V5(v5) => {
                        println!("Received Netflow V5 packet:");
                        println!("  Version: {}", v5.header.version);
                        println!("  Flow count: {}", v5.header.count);
                        println!("  System uptime: {:?}", v5.header.sys_up_time);
                        println!("  Unix seconds: {}", v5.header.unix_secs);

                        for flowset in v5.flowsets {
                            let src_ip = IpAddr::V4(flowset.src_addr);
                            let dst_ip = IpAddr::V4(flowset.dst_addr);

                            let (src_country, src_as) = enricher_clone.enrich(src_ip);
                            let (dst_country, dst_as) = enricher_clone.enrich(dst_ip);

                            println!("  Flow:");
                            println!("    Source IP: {}, Country: {:?}, AS: {:?}", src_ip, src_country, src_as);
                            println!("    Dest IP: {}, Country: {:?}, AS: {:?}", dst_ip, dst_country, dst_as);
                            println!("    Bytes: {}", flowset.d_octets);
                            println!("    Packets: {}", flowset.d_pkts);
                            println!("    Start time: {:?}", flowset.first);
                            println!("    End time: {:?}", flowset.last);
                            println!("    Source port: {}", flowset.src_port);
                            println!("    Destination port: {}", flowset.dst_port);
                            println!("    Protocol: {}", flowset.protocol_number);
                        }
                    }
                    NetflowPacketResult::IPFix(ipfix) => {
                        println!("Received IPFIX packet:");
                        println!("  Version: {}", ipfix.header.version);
                        println!("  Length: {}", ipfix.header.length);
                        println!("  Export Time: {:?}", ipfix.header.export_time);

                        for flowset in &ipfix.flowsets {
                            match &flowset.body {
                                FlowSetBody { data: Some(data), .. } => {
                                    for record in &data.data_fields {
                                        let src_ip = record.get(&(IPFixField::SourceIpv4address as usize))
                                            .and_then(|(_, value)| if let FieldValue::Ip4Addr(ip) = value { Some(*ip) } else { None });
                                        let dst_ip = record.get(&(IPFixField::DestinationIpv4address as usize))
                                            .and_then(|(_, value)| if let FieldValue::Ip4Addr(ip) = value { Some(*ip) } else { None });

                                        if let (Some(src_ip), Some(dst_ip)) = (src_ip, dst_ip) {
                                            let (src_country, src_as) = enricher_clone.enrich(IpAddr::V4(src_ip));
                                            let (dst_country, dst_as) = enricher_clone.enrich(IpAddr::V4(dst_ip));

                                            println!("  Flow:");
                                            println!("    Source IP: {}, Country: {:?}, AS: {:?}", src_ip, src_country, src_as);
                                            println!("    Dest IP: {}, Country: {:?}, AS: {:?}", dst_ip, dst_country, dst_as);

                                            if let Some((_, FieldValue::DataNumber(DataNumber::U64(bytes)))) = record.get(&(IPFixField::OctetDeltaCount as usize)) {
                                                println!("    Bytes: {}", bytes);
                                            }
                                            if let Some((_, FieldValue::DataNumber(DataNumber::U64(packets)))) = record.get(&(IPFixField::PacketDeltaCount as usize)) {
                                                println!("    Packets: {}", packets);
                                            }
                                        }
                                    }
                                },
                                _ => println!("  Non-data FlowSet (template or options)"),
                            }
                        }
                    }
                    NetflowPacketResult::Error(e) => {
                        println!("Error parsing packet: {:?}", e);
                    }
                    _ => {
                        println!("Received unsupported packet type");
                    }
                }
            }
        });
    }
}