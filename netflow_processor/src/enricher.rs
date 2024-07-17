use cidr::{IpCidr, Ipv4Cidr};
use std::collections::BTreeMap;
use std::path::Path;
use anyhow::Result;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct DataEnricher {
    country_map: BTreeMap<IpCidr, String>,
    as_map: BTreeMap<Ipv4Cidr, u32>
}

impl DataEnricher {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            country_map: Self::load_country_map("src/data/ipv4")?,
            as_map: Self::load_as_map("src/data/ip_to_as.txt")?,
        })

    }

    fn load_country_map(directory: &str) -> Result<BTreeMap<IpCidr, String>, Box<dyn std::error::Error>> {
        let mut map = BTreeMap::new();
        let dir_path = Path::new(directory);

        if dir_path.is_dir() {
            for entry in fs::read_dir(dir_path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("cidr") {
                    let country_code = path.file_stem()
                        .and_then(|s| s.to_str())
                        .ok_or("Invalid filename")?
                        .to_uppercase();

                    let file = File::open(&path)?;
                    let reader = BufReader::new(file);
                    for line in reader.lines() {
                        let line = line?;
                        let cidr = IpCidr::from_str(line.trim())?;
                        map.insert(cidr, country_code.clone());
                    }
                }
            }
        }
        info!("Loaded country map");
        Ok(map)
    }
    fn load_as_map(file_path: &str) -> Result<BTreeMap<Ipv4Cidr, u32>, Box<dyn std::error::Error>> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let mut map = BTreeMap::new();

        for (line_number, line) in reader.lines().enumerate() {
            let line = line?;
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() == 2 {
                let ip_range: Vec<&str> = parts[0].split('/').collect();
                if ip_range.len() != 2 {
                    warn!("Invalid IP range format on line {}: {}", line_number + 1, line);
                    continue;
                }

                match (Ipv4Addr::from_str(ip_range[0]), Ipv4Addr::from_str(ip_range[1]), parts[1].parse::<u32>()) {
                    (Ok(start_ip), Ok(end_ip), Ok(as_number)) => {
                        if start_ip <= end_ip {
                            let cidrs = Self::ip_range_to_cidrs(start_ip, end_ip);
                            if cidrs.is_empty() {
                                warn!("No valid CIDRs generated for IP range on line {}: {}", line_number + 1, line);
                            } else {
                                for cidr in cidrs {
                                    map.insert(cidr, as_number);
                                }
                            }
                        } else {
                            warn!("Invalid IP range (start > end) on line {}: {}", line_number + 1, line);
                        }
                    },
                    (Err(e), _, _) | (_, Err(e), _) => {
                        warn!("Error parsing IP address on line {}: {}. Line content: {}", line_number + 1, e, line);
                    },
                    (_, _, Err(e)) => {
                        warn!("Error parsing AS number on line {}: {}. Line content: {}", line_number + 1, e, line);
                    }
                }
            } else {
                warn!("Invalid line format at line {}: {}", line_number + 1, line);
            }
        }

        if map.is_empty() {
            error!("No valid entries found in AS map file");
            return Err("No valid entries found in AS map file".into());
        }

        info!("Loaded AS map with {} entries", map.len());
        Ok(map)
    }

    fn ip_range_to_cidrs(start: Ipv4Addr, end: Ipv4Addr) -> Vec<Ipv4Cidr> {
        let mut result = Vec::new();
        let mut current = u32::from(start);
        let end_u32 = u32::from(end);

        while current <= end_u32 {
            let prefix_len = (32 - (end_u32 - current + 1).leading_zeros()).max(0);
            let max_prefix_len = 32 - current.trailing_zeros();
            let prefix_len = prefix_len.min(max_prefix_len);

            // Create a mask for the network part
            let mask = !0u32 << (32 - prefix_len);
            // Apply the mask to get the network address
            let network = current & mask;

            match Ipv4Cidr::new(Ipv4Addr::from(network), prefix_len as u8) {
                Ok(cidr) => {
                    result.push(cidr);
                    let last_addr = u32::from(cidr.last_address());
                    if last_addr == u32::MAX {
                        // We've reached the maximum IP address, so we're done
                        break;
                    }
                    current = last_addr.saturating_add(1);
                },
                Err(e) => {
                    warn!("Error creating CIDR: {}. Skipping IP range {:?}-{:?}", e, Ipv4Addr::from(current), Ipv4Addr::from(end_u32));
                    // Move to the next IP address
                    if current == u32::MAX {
                        // We've reached the maximum IP address, so we're done
                        break;
                    }
                    current = current.saturating_add(1);
                }
            }
        }

        result
    }

    pub fn enrich(&self, ip: IpAddr) -> (Option<String>, Option<u32>) {
        let country = self.country_map
            .range(..=IpCidr::new(ip, 32).unwrap())
            .next_back()
            .filter(|(cidr, _)| cidr.contains(&ip))
            .map(|(_, country)| country.clone());

        let as_number = if let IpAddr::V4(ipv4) = ip {
            self.as_map
                .range(..=Ipv4Cidr::new(ipv4, 32).unwrap())
                .next_back()
                .filter(|(cidr, _)| cidr.contains(&ipv4))
                .map(|(_, as_num)| *as_num)
        } else {
            None
        };

        (country, as_number)
    }
}

