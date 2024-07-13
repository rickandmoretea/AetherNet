use netflow_parser::{NetflowPacketResult};
use netflow_parser::static_versions::v5::{V5, Header as V5Header, FlowSet as V5FlowSet};
use netflow_parser::variable_versions::ipfix::{IPFix, Header as IPFixHeader, FlowSet as IPFixFlowSet, FlowSetBody};
use rand::Rng;
use std::net::{UdpSocket, Ipv4Addr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use netflow_parser::protocol::ProtocolTypes;

fn generate_random_ip() -> Ipv4Addr {
    let mut rng = rand::thread_rng();
    Ipv4Addr::new(rng.gen(), rng.gen(), rng.gen(), rng.gen())
}

fn main() -> std::io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    let netflow_collector = "127.0.0.1:2055";
    let mut rng = rand::thread_rng();

    loop {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        let packet: NetflowPacketResult = if rng.gen_bool(0.5) {
            // Generate V5 packet
            NetflowPacketResult::V5(V5 {
                header: V5Header {
                    version: 5,
                    count: 1,
                    sys_up_time: Duration::from_secs(now.as_secs()),
                    unix_secs: now.as_secs() as u32,
                    unix_nsecs: now.subsec_nanos(),
                    flow_sequence: 0,
                    engine_type: 0,
                    engine_id: 0,
                    sampling_interval: 0,
                },
                flowsets: vec![V5FlowSet {
                    src_addr: generate_random_ip(),
                    dst_addr: generate_random_ip(),
                    next_hop: Ipv4Addr::new(0, 0, 0, 0),
                    input: 0,
                    output: 0,
                    d_pkts: rng.gen(),
                    d_octets: rng.gen(),
                    first: now,
                    last: now,
                    src_port: rng.gen(),
                    dst_port: rng.gen(),
                    pad1: 0,
                    tcp_flags: 0,
                    protocol_number: 6,
                    tos: 0,
                    src_as: rng.gen(),
                    dst_as: rng.gen(),
                    src_mask: 24,
                    dst_mask: 24,
                    pad2: 0,
                    protocol_type: ProtocolTypes::Hopopt,
                }],
            })
        } else {
            // Generate IPFIX packet
            NetflowPacketResult::IPFix(IPFix {
                header: IPFixHeader {
                    version: 10,
                    length: 0, // This will be calculated automatically
                    export_time: Duration::from_secs(now.as_secs()),
                    sequence_number: rng.gen(),
                    observation_domain_id: rng.gen(),
                },
                flowsets: vec![IPFixFlowSet {
                    header: netflow_parser::variable_versions::ipfix::FlowSetHeader {
                        header_id: 256, // Data FlowSet ID
                        length: 0, // This will be calculated automatically
                    },
                    body: FlowSetBody {
                        template: None,
                        options_template: None,
                        data: None,
                        options_data: None,
                    },
                }],
            })
        };

        let bytes = match &packet {
            NetflowPacketResult::V5(v5) => v5.to_be_bytes(), // Ensure this method exists or implement it
            NetflowPacketResult::IPFix(ipfix) => ipfix.to_be_bytes(), // Ensure this method exists or implement it
            _ => panic!("Unsupported version"),
        };
        socket.send_to(&bytes, netflow_collector)?;
        println!("Sent {:?} packet", packet);

        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
