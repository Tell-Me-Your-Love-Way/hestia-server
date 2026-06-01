mod config;
use config::HostConfig;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{info, error, debug};
use reqwest::Client;
use futures_util::StreamExt;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("backup_server=debug").init();
    info!("Iniciando Servidor de Backup HTTP/3...");
    
    let client = Client::builder()
        .http3_prior_knowledge()
        .danger_accept_invalid_certs(true)
        .build()?;
    
    let hosts = config::load_all_hosts().expect("Erro ao ler a pasta 'hosts/'");
    let sched = JobScheduler::new().await?;
    for host in hosts {
        let http_client = client.clone();
        let h = host.clone();
        let cron_schedule = h.cron_schedule.clone();

        let job = Job::new_async(cron_schedule.as_str(), move |_uuid, _l| {
            let http = http_client.clone();
            let target = h.clone();
            
            Box::pin(async move {
                info!(host = %target.id, "--- INICIANDO JOB DE BACKUP (HTTP/3) ---");
                
                if let Err(e) = execute_backup(http, &target).await {
                    error!(host = %target.id, "Pane no processo de backup: {:?}", e);
                } else {
                    info!(host = %target.id, "--- BACKUP CONCLUÍDO COM SUCESSO ---");
                }
            })
        })?;

        sched.add(job).await?;
        debug!(host = %host.id, schedule = %host.cron_schedule, "Host agendado.");
    }

    sched.start().await?;
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn execute_backup(client: Client, host: &HostConfig) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{}/api", host.endpoint);
    
    let output_dir = format!("storage/{}", host.id);
    fs::create_dir_all(&output_dir)?;

    for path in &host.paths {
        debug!(host = %host.id, path = %path, url = %url, "Enviando request de backup...");

        let mut payload = HashMap::new();
        payload.insert("path", path.clone());

        let response = client.post(&url)
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            error!(host = %host.id, path = %path, status = %response.status(), "Agente respondeu com erro ao processar o backup.");
            continue;
        }
        
        let mut download_filename = None;
        if let Some(content_disposition) = response.headers().get("content-disposition") {
            if let Ok(cd_str) = content_disposition.to_str() {
                if let Some(pos) = cd_str.find("filename=\"") {
                    let start = pos + 10;
                    if let Some(end) = cd_str[start..].find('"') {
                        download_filename = Some(cd_str[start..start+end].to_string());
                    }
                }
            }
        }
        
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        let filename = match download_filename {
            Some(name) => {
                if name.ends_with(".tar.zst") {
                    let stem = &name[..name.len() - 8];
                    format!("{}-{}.tar.zst", stem, timestamp)
                } else {
                    format!("{}-{}.tar.zst", name, timestamp)
                }
            }
            None => {
                let sanitized_path = path.replace("/", "_").replace("\\", "_").replace(":", "-Disk_");
                format!("backup-{}-{}.tar.zst", sanitized_path, timestamp)
            }
        };

        let file_path = format!("{}/{}", output_dir, filename);
        let mut file = File::create(&file_path)?;
        let mut stream = response.bytes_stream();

        debug!(host = %host.id, path = %path, "Recebendo payload do arquivo, escrevendo no disco...");
        
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result?;
            file.write_all(&chunk)?;
        }

        info!(host = %host.id, path = %path, destino = %file_path, "Arquivo salvo e integrado com sucesso.");
    }
    
    if host.retention_versions > 0 {
        if let Err(e) = apply_retention(&output_dir, host.retention_versions) {
            error!(host = %host.id, "Erro ao aplicar retenção: {:?}", e);
        }
    }

    Ok(())
}

fn apply_retention(output_dir: &str, retention_limit: usize) -> Result<(), Box<dyn std::error::Error>> {
    let entries = fs::read_dir(output_dir)?;
    let mut files = Vec::new();

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                if filename.ends_with(".tar.zst") {
                    if let Ok(metadata) = entry.metadata() {
                        if let Ok(modified) = metadata.modified() {
                            files.push((path.clone(), filename.to_string(), modified));
                        }
                    }
                }
            }
        }
    }

    if files.is_empty() {
        return Ok(());
    }
    
    let mut groups: HashMap<String, Vec<(PathBuf, std::time::SystemTime)>> = HashMap::new();

    for (full_path, filename, modified) in files {
        let stem = &filename[..filename.len() - 8];
        let prefix = if let Some(last_dash_idx) = stem.rfind('-') {
            &stem[..last_dash_idx]
        } else {
            stem
        };

        groups.entry(prefix.to_string())
            .or_default()
            .push((full_path, modified));
    }
    
    for (prefix, mut group_files) in groups {
        if group_files.len() > retention_limit {
            group_files.sort_by_key(|&(_, modified)| modified);

            let delete_count = group_files.len() - retention_limit;
            info!(
                group = %prefix,
                limit = retention_limit,
                total = group_files.len(),
                delete = delete_count,
                "Aplicando política de retenção para backup"
            );

            for i in 0..delete_count {
                let (file_to_delete, _) = &group_files[i];
                info!(arquivo = ?file_to_delete, "Removendo backup antigo devido à política de retenção...");
                fs::remove_file(file_to_delete)?;
            }
        }
    }

    Ok(())
}