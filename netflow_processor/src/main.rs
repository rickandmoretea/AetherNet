mod enricher;

use std::sync::Arc;
use anyhow::Result;
use netflow_parser::{NetflowParser,NetflowPacketResult};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "0.0.0.0:2055";
    let socket = UdpSocket::bind(addr).await?;
    println!("Listening on: {}", addr);

    let parser = Arc::new(Mutex::new(NetflowParser::default()));
    let mut buf = [0u8; 1500]; // 1500 is the maximum size of an Ethernet frame

    loop {
        let (len, _) = socket.recv_from(&mut buf).await?;
        let parser_clone = Arc::clone(&parser);
        let packet_data = buf[..len].to_vec();

        tokio::spawn(async move {
            let mut parser = parser_clone.lock().await;
            let parsed_packets = parser.parse_bytes(&packet_data);

            for packet in parsed_packets {
                match packet {
                    NetflowPacketResult::V5(v5) => {
                        println!("Received Netflow V5 packet: {:?}", v5);
                    }
                    NetflowPacketResult::V7(v7) => {
                        println!("Received Netflow V7 packet: {:?}", v7);

                    }
                    NetflowPacketResult::V9(v9) => {
                        println!("Received Netflow V9 packet: {:?}", v9);
                    }
                    NetflowPacketResult::IPFix(ipfix) => {
                        println!("Received IPFIX packet: {:?}", ipfix);
                    }

                    NetflowPacketResult::Error(e) => {
                        println!("Error parsing packet: {:?}", e);
                    }
                }
            }
        });
    }


}
