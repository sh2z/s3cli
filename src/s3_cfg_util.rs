use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub user: String,
    pub url: String,
    pub description: Option<String>,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CephKeysConfig {
    pub default_account: String,
    pub accounts: Vec<Account>,
}

impl CephKeysConfig {
    /// 从文件加载配置
    pub fn load() -> Result<Self> {
        let config_path = CephKeysConfig::find_config_file()?;
        let content = fs::read_to_string(&config_path)
            .context(format!("Failed to read config file: {:?}", config_path))?;
        let config: CephKeysConfig = serde_yaml::from_str(&content)
            .context("Failed to parse YAML config")?;
        Ok(config)
    }

    /// 获取指定用户的配置
    pub fn get_account(&self, user: &str) -> Result<&Account> {
        self.accounts
            .iter()
            .find(|a| a.user == user)
            .ok_or_else(|| anyhow!("Account '{}' not found in config", user))
    }

    /// 获取默认用户配置
    pub fn get_default_account(&self) -> Result<&Account> {
        self.get_account(&self.default_account)
    }

    fn find_config_file() -> Result<PathBuf> {
        // 优先从 ~/.config/s3cli/ceph_keys.yaml 读取
        if let Some(home_dir) = dirs::home_dir() {
            let config_dir = home_dir.join(".config").join("s3cli");
            let config_path = config_dir.join("ceph_keys.yaml");
            if config_path.exists() {
                return Ok(config_path);
            }
        }

        // 回退到项目根目录的 ceph_keys.yaml（用于开发）
        let current_dir_cfg = std::env::current_dir()?.join("ceph_keys.yaml");
        if current_dir_cfg.exists() {
            return Ok(current_dir_cfg);
        }

        Err(anyhow!(
            "Configuration file not found. Expected at ~/.config/s3cli/ceph_keys.yaml"
        ))
    }
}

/// 获取指定用户的 S3 配置
pub fn get_account_config(user: Option<&str>) -> Result<Account> {
    let config = CephKeysConfig::load()?;
    
    match user {
        Some(u) => config.get_account(u).cloned(),
        None => config.get_default_account().cloned(),
    }
}

/// 解析 bucket URL，支持 s3://bucket 或 bucket 两种形式
/// 返回纯 bucket 名称
pub fn parse_bucket_url(bucket_url: &str) -> Result<String> {
    let bucket_url = bucket_url.trim();
    
    if bucket_url.is_empty() {
        return Err(anyhow::anyhow!("Bucket URL cannot be empty"));
    }
    
    // 如果以 s3:// 开头，去掉前缀
    let bucket = if let Some(stripped) = bucket_url.strip_prefix("s3://") {
        stripped
    } else {
        bucket_url
    };
    
    // 如果包含/，只取第一部分（bucket 名）
    let bucket_name = bucket.split('/').next().ok_or_else(|| anyhow::anyhow!("Invalid bucket URL"))?;
    
    if bucket_name.is_empty() {
        return Err(anyhow::anyhow!("Bucket name cannot be empty"));
    }
    
    Ok(bucket_name.to_string())
}
