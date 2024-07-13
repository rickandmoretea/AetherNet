use cidr::IpCidr;
use std::collections::BTreeMap;
use std::path::Path;
use anyhow::Result;
use std::fs;
use std::io::{BufRead, BufReader};
use std::net::IpAddr;
use std::str::FromStr;

#[derive(Clone)]
pub struct DataEnricher {
    country_map: BTreeMap<IpCidr, String>,
    as_map: BTreeMap<IpCidr, u32>
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

                    let file = fs::File::open(&path)?;
                    let reader = BufReader::new(file);
                    for line in reader.lines() {
                        let line = line?;
                        let cidr = IpCidr::from_str(line.trim())?;
                        map.insert(cidr, country_code.clone());
                    }
                }
            }
        }

        Ok(map)
    }
    fn load_as_map(file_path: &str) -> Result<BTreeMap<IpCidr, u32>, Box<dyn std::error::Error>> {
        let file = fs::File::open(file_path)?;
        let reader = BufReader::new(file);
        let mut map = BTreeMap::new();

        for line in reader.lines() {
            let line = line?;
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() == 2 {
                let cidr = IpCidr::from_str(parts[0])?;
                let as_number = parts[1].parse::<u32>()?;
                map.insert(cidr, as_number);
            }
        }

        Ok(map)
    }

    pub fn enrich(&self, ip: IpAddr) -> (Option<String>, Option<u32>) {
        let country = self.country_map
            .range(..=IpCidr::new(ip, 32).unwrap())
            .next_back()
            .filter(|(cidr, _)| cidr.contains(&ip))
            .map(|(_, country)| country.clone());

        let as_number = self.as_map
            .range(..=IpCidr::new(ip, 32).unwrap())
            .next_back()
            .filter(|(cidr, _)| cidr.contains(&ip))
            .map(|(_, as_num)| *as_num);

        (country, as_number)
    }
}