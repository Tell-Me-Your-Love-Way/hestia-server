use serde::{Deserialize, Serialize};
use std::fs::File;
use glob::glob;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HostConfig {
    pub id: String,
    pub endpoint: String,
    pub cron_schedule: String,
    pub paths: Vec<String>,
    pub retention_versions: usize,
}

pub fn load_all_hosts() -> Result<Vec<HostConfig>, Box<dyn std::error::Error>> {
    let mut hosts = Vec::new();
    
    for entry in glob("hosts/*.yaml")?.chain(glob("hosts/*.yml")?) {
        let path = entry?;
        let file = File::open(path)?;
        let host: HostConfig = serde_yaml::from_reader(file)?;
        hosts.push(host);
    }
    
    Ok(hosts)
}