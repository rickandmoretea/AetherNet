use cidr::Ipv4Cidr;
use std::collections::HashMap;

pub struct DataEnricher {
    country_map: HashMap<Ipv4Cidr, String>,
    as_map: HashMap<Ipv4Cidr, u32>
}

impl DataEnricher {
    pub fn new() -> Self {
        // TODO: load IP to country and IP to AS mappings
        Self {
            country_map: HashMap::new(),
            as_map: HashMap::new(),
        }
    }

    pub fn enricher(&self, ip: std::net::Ipv4Addr) -> (Option<String>, Option<u32>) {
        // TODO: Implement CIDR matching logic
        (None, None)
    }
}