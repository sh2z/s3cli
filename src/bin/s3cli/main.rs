use cli::s3_async_util::*;
use cli::s3_cfg_util::*;

use anyhow::Result;
use anyhow::anyhow;
use clap::{Parser, Subcommand};
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, Color, Table};
use dialoguer::{Input, Select, Confirm, theme::ColorfulTheme};
use owo_colors::OwoColorize;
use std::time::SystemTime;

#[derive(Parser, Debug)]
#[command(
    author = "sh2z",
    version = "3.0",
    about = "s3cli - Ceph RGW 客户端工具",
    long_about = "支持多用户的 Ceph RGW 命令行工具"
)]
struct Params {
    /// 用户名（可选，省略时使用 default_account）
    #[arg(index = 1)]
    user: Option<String>,

    /// 子命令
    #[command(subcommand)]
    command: Option<SubCommand>,
}

#[derive(Subcommand, Debug)]
enum SubCommand {
    /// 显示所有账户信息
    Show,

    /// 交互式配置管理
    Config,

    /// 创建存储桶
    Mb {
        #[arg(help = "存储桶名称 (s3://bucket 或 bucket)")]
        bucket: String,
    },

    /// 删除存储桶
    Rb {
        #[arg(help = "存储桶名称 (s3://bucket 或 bucket)")]
        bucket: String,
    },

    /// 列出存储桶或对象（格式同 s3cmd）
    Ls {
        #[arg(help = "存储桶名称 (s3://bucket 或 bucket)")]
        bucket: Option<String>,
        #[arg(help = "前缀")]
        prefix: Option<String>,
    },

    /// 上传文件
    Put {
        #[arg(help = "本地文件路径")]
        local_file: String,
        #[arg(help = "存储桶 URL (s3://bucket 或 bucket)")]
        bucket: String,
        #[arg(help = "对象键（可选，默认为本地文件路径）")]
        key: Option<String>,
    },

    /// 下载文件
    Get {
        #[arg(help = "存储桶 URL (s3://bucket 或 bucket)")]
        bucket: String,
        #[arg(help = "对象键")]
        key: String,
        #[arg(help = "本地文件路径")]
        local_file: String,
    },

    /// 递归上传目录
    Putr {
        #[arg(help = "本地文件夹路径")]
        local_dir: String,
        #[arg(help = "存储桶 URL (s3://bucket 或 bucket)")]
        bucket: String,
        #[arg(help = "前缀")]
        prefix: Option<String>,
    },

    /// 递归下载目录
    Getr {
        #[arg(help = "存储桶 URL (s3://bucket 或 bucket)")]
        bucket: String,
        #[arg(help = "前缀")]
        prefix: Option<String>,
        #[arg(help = "本地文件夹路径")]
        local_dir: String,
    },

    /// 复制对象
    Cp {
        #[arg(help = "源存储桶 URL (s3://bucket 或 bucket)")]
        src_bucket: String,
        #[arg(help = "源对象键")]
        src_key: String,
        #[arg(help = "目标存储桶 URL (s3://bucket 或 bucket)")]
        dst_bucket: String,
        #[arg(help = "目标对象键")]
        dst_key: String,
    },

    /// 移动对象
    Mv {
        #[arg(help = "源存储桶 URL (s3://bucket 或 bucket)")]
        src_bucket: String,
        #[arg(help = "源对象键")]
        src_key: String,
        #[arg(help = "目标存储桶 URL (s3://bucket 或 bucket)")]
        dst_bucket: String,
        #[arg(help = "目标对象键")]
        dst_key: String,
    },

    /// 删除对象
    Del {
        #[arg(help = "存储桶 URL (s3://bucket 或 bucket)")]
        bucket: String,
        #[arg(help = "key")]
        key: String,
    },

    /// 递归删除对象
    Delr {
        #[arg(help = "存储桶 URL (s3://bucket 或 bucket)")]
        bucket: String,
        #[arg(help = "前缀")]
        prefix: Option<String>,
    },

    /// 显示信息
    Info {
        #[arg(help = "存储桶 URL (s3://bucket 或 bucket)")]
        bucket: String,
        #[arg(help = "对象键")]
        key: Option<String>,
    },

    /// 设置 MIME 类型
    Mime {
        #[arg(help = "存储桶 URL (s3://bucket 或 bucket)")]
        bucket: String,
        #[arg(help = "前缀")]
        prefix: Option<String>,
        #[arg(help = "MIME_TYPE")]
        mime: String,
    },

    /// 生成签名 URL
    Signurl {
        #[arg(help = "存储桶 URL (s3://bucket 或 bucket)")]
        bucket: String,
        #[arg(help = "对象键")]
        key: String,
        #[arg(default_value = "3600", help = "有效期 (秒)")]
        expires: u64,
    },

    /// 获取对象原生 URL
    Url {
        #[arg(help = "存储桶 URL (s3://bucket 或 bucket)")]
        bucket: String,
        #[arg(help = "对象键")]
        key: String,
    },

    /// 设置桶为公有访问（需要 admin 权限）
    Public {
        #[arg(help = "桶 URL，如 s3://bucket")]
        bucket_url: String,
    },

    /// 设置桶过期时间（需要 admin 权限）
    Expire {
        #[arg(help = "桶 URL（只接受 bucket，如 s3://bucket）")]
        bucket_url: String,
        #[arg(help = "过期天数")]
        days: i32,
    },

    /// 设置生命周期规则（需要 admin 权限）
    Lifecycle {
        #[arg(help = "过期天数")]
        days: i32,
        #[arg(help = "桶路径，如 s3://bucket/prefix")]
        bucket_path: String,
    },
}

/// 显示所有账户信息的表格
async fn show_accounts_table() -> Result<()> {
    let config = CephKeysConfig::load()?;

    // 创建表格
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_header(vec![
            Cell::new("USER").fg(Color::Cyan),
            Cell::new("ACCESS_KEY").fg(Color::Cyan),
            Cell::new("SECRET_KEY").fg(Color::Cyan),
            Cell::new("URL").fg(Color::Cyan),
            Cell::new("BUCKETS").fg(Color::Cyan),
        ])
        .set_content_arrangement(comfy_table::ContentArrangement::Dynamic);

    // 为每个账户获取桶列表并添加到表格
    for account in &config.accounts {
        let s3_client = S3Client::new(&account.access_key, &account.secret_key, &account.url).await;
        let buckets_result = s3_client.list_buckets().await;

        let buckets_str = match buckets_result {
            Ok(buckets) => {
                if buckets.is_empty() {
                    "(no buckets)".to_string()
                } else {
                    buckets.join(", ")
                }
            }
            Err(e) => format!("(error: {})", e),
        };

        table.add_row(vec![
            Cell::new(&account.user).fg(Color::Green),
            Cell::new(&account.access_key),
            Cell::new(&account.secret_key),
            Cell::new(&account.url),
            Cell::new(&buckets_str),
        ]);
    }

    println!("{}", table);
    Ok(())
}

/// 格式化时间为 s3cmd 风格：YYYY-MM-DD HH:MM（17 个字符，左对齐）
fn format_time_s3cmd(timestamp: Option<&SystemTime>) -> String {
    match timestamp {
        Some(t) => {
            // 转换为本地时间并格式化
            let datetime: chrono::DateTime<chrono::Local> = (*t).into();
            format!("{:<17}", datetime.format("%Y-%m-%d %H:%M").to_string())
        }
        None => "                 ".to_string(),
    }
}

/// 以 s3cmd 风格打印对象列表（显示用户信息，使用原始字节数）
fn print_objects_s3cmd_format(account: &Account, objects: &[ObjectInfo]) {
    // 首先显示用户信息（同 s3 命令）
    println!("user: \"{}\"", account.user);
    println!("description: \"{}\"", account.description.as_deref().unwrap_or(""));
    println!("access_key: \"{}\"", account.access_key);
    println!("secret_key: \"{}\"", account.secret_key);
    println!("url: \"{}\"", account.url);
    println!();

    for obj in objects {
        if obj.is_dir {
            // 目录格式：                          DIR  s3://bucket/path/
            println!("{:>20}  {}", "DIR", obj.path);
        } else {
            // 文件格式：2025-03-31 04:45        33240  s3://bucket/file
            let time_str = format_time_s3cmd(obj.last_modified.as_ref());
            println!("{}  {:>10}  {}", time_str, obj.size, obj.path);
        }
    }
}

/// 交互式配置管理器
mod config_editor {
    use super::*;

    pub struct Editor;

    impl Editor {
        pub fn run() -> Result<()> {
            let mut config = CephKeysConfig::load()?;

            loop {
                println!("\n{}", "⚙️  S3 配置管理\n".bold());

                let items = vec![
                    "添加账户",
                    "删除账户",
                    "设置默认账户",
                    "查看配置",
                    "退出",
                ];

                let selection = Select::with_theme(&ColorfulTheme::default())
                    .with_prompt("选择操作")
                    .items(&items)
                    .default(0)
                    .interact()?;

                match selection {
                    0 => Self::add_account(&mut config)?,
                    1 => Self::delete_account(&mut config)?,
                    2 => {
                        Self::set_default_account(&mut config)?;
                        break;
                    }
                    3 => Self::view_config(&config)?,
                    4 => {
                        println!("{} 退出配置管理", "✓".green().bold());
                        break;
                    }
                    _ => unreachable!(),
                }

                println!();
            }

            Ok(())
        }

        fn add_account(config: &mut CephKeysConfig) -> Result<()> {
            println!("\n{}", "➕ 添加账户".bold());

            // 输入用户名
            let user: String = Input::with_theme(&ColorfulTheme::default())
                .with_prompt("用户名")
                .validate_with(|input: &String| -> Result<(), &str> {
                    if input.is_empty() {
                        Err("用户名不能为空")
                    } else if config.accounts.iter().any(|a| a.user == *input) {
                        Err("用户名已存在")
                    } else {
                        Ok(())
                    }
                })
                .interact_text()?;

            // 输入 URL
            let url: String = Input::with_theme(&ColorfulTheme::default())
                .with_prompt("S3 URL")
                .default("https://s3.example.com".to_string())
                .interact_text()?;

            // 输入描述
            let description: String = Input::with_theme(&ColorfulTheme::default())
                .with_prompt("描述（可选）")
                .allow_empty(true)
                .interact_text()?;

            // 输入 Access Key
            let access_key: String = Input::with_theme(&ColorfulTheme::default())
                .with_prompt("Access Key")
                .interact_text()?;

            // 输入 Secret Key
            let secret_key: String = Input::with_theme(&ColorfulTheme::default())
                .with_prompt("Secret Key")
                .interact_text()?;

            // 确认添加
            let confirm = Confirm::with_theme(&ColorfulTheme::default())
                .with_prompt("确认添加此账户？")
                .default(true)
                .interact()?;

            if !confirm {
                println!("{} 取消添加账户", "✗".red().bold());
                return Ok(());
            }

            // 创建新账户
            let account = Account {
                user: user.clone(),
                url,
                description: if description.is_empty() { None } else { Some(description) },
                access_key,
                secret_key,
            };

            config.accounts.push(account);
            save_config(config)?;

            println!("\n{} 账户 '{}' 添加成功", "✓".green().bold(), user.cyan());

            Ok(())
        }

        fn delete_account(config: &mut CephKeysConfig) -> Result<()> {
            println!("\n{}", "🗑️  删除账户".bold());

            if config.accounts.is_empty() {
                println!("{} 没有可删除的账户", "✗".red().bold());
                return Ok(());
            }

            // 构建账户列表
            let mut account_items: Vec<String> = config.accounts.iter()
                .map(|a| {
                    let is_default = a.user == config.default_account;
                    if is_default {
                        format!("★ {}（{}） - 默认账户（不可删除）", a.user.cyan(), a.url.dimmed())
                    } else {
                        format!("  {}（{}）", a.user.cyan(), a.url.dimmed())
                    }
                })
                .collect();

            // 增加返回上一级选项
            account_items.push("返回上一级".to_string());

            let selection = Select::with_theme(&ColorfulTheme::default())
                .with_prompt("选择要删除的账户")
                .items(&account_items)
                .default(0)
                .interact()?;

            // 如果选择最后一项（返回上一级），直接返回
            if selection == account_items.len() - 1 {
                return Ok(());
            }

            let deleted_name = config.accounts[selection].user.clone();
            let is_default_account = deleted_name == config.default_account;

            // 禁止删除默认账户
            if is_default_account {
                println!("{} 不能删除默认账户 '{}'", "✗".red().bold(), deleted_name.cyan());
                println!("{} 请先切换默认账户，再删除该账户", "ℹ️".blue().bold());
                return Ok(());
            }

            // 确认删除
            let confirm = Confirm::with_theme(&ColorfulTheme::default())
                .with_prompt(&format!("确认删除账户 '{}'？", deleted_name))
                .default(false)
                .interact()?;

            if !confirm {
                println!("{} 取消删除", "✗".red().bold());
                return Ok(());
            }

            // 删除账户
            config.accounts.remove(selection);
            save_config(config)?;

            println!("\n{} 账户 '{}' 已删除", "✓".green().bold(), deleted_name.cyan());

            Ok(())
        }

        fn set_default_account(config: &mut CephKeysConfig) -> Result<()> {
            println!("\n{}", "🎯 设置默认账户".bold());

            if config.accounts.is_empty() {
                println!("{} 没有可用的账户", "✗".red().bold());
                return Ok(());
            }

            // 构建账户列表
            let mut account_items = Vec::new();
            let mut default_idx = 0;

            for (idx, account) in config.accounts.iter().enumerate() {
                let is_default = account.user == config.default_account;
                let display = if is_default {
                    format!("★ {}", account.user.cyan())
                } else {
                    format!("  {}", account.user.cyan())
                };
                account_items.push(display);

                if is_default {
                    default_idx = idx;
                }
            }

            let selection = Select::with_theme(&ColorfulTheme::default())
                .with_prompt("选择默认账户")
                .items(&account_items)
                .default(default_idx)
                .interact()?;

            let new_default = config.accounts[selection].user.clone();
            config.default_account = new_default.clone();
            save_config(config)?;

            println!("\n{} 默认账户已设置为：{}", "✓".green().bold(), new_default.cyan());

            Ok(())
        }

        fn view_config(config: &CephKeysConfig) -> Result<()> {
            println!("\n{}", "📋 当前配置".bold());
            println!();
            println!("默认账户：{}", config.default_account.cyan());
            println!();
            println!("所有账户:");
            for account in &config.accounts {
                let is_default = account.user == config.default_account;
                let star = if is_default { "★" } else { " " };
                println!(
                    "  {} {} - {} ({})",
                    star,
                    account.user.cyan(),
                    account.url.dimmed(),
                    account.description.as_deref().unwrap_or("无描述").dimmed()
                );
            }
            println!();
            println!("配置文件位置：~/.config/s3cli/ceph_keys.yaml");

            Ok(())
        }
    }

    fn save_config(config: &CephKeysConfig) -> Result<()> {
        use std::fs;

        // 获取配置文件路径
        let config_path = if let Some(home_dir) = dirs::home_dir() {
            let config_dir = home_dir.join(".config").join("s3cli");
            fs::create_dir_all(&config_dir)?;
            config_dir.join("ceph_keys.yaml")
        } else {
            return Err(anyhow!("Failed to get home directory"));
        };

        // 序列化为 YAML
        let yaml_content = serde_yaml::to_string(config)?;
        fs::write(&config_path, yaml_content)?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let params = Params::parse();

    // 处理 show 命令（不需要用户参数）
    if let Some(SubCommand::Show) = &params.command {
        show_accounts_table().await?;
        return Ok(());
    }

    // 处理 config 命令（不需要用户参数）
    if let Some(SubCommand::Config) = &params.command {
        config_editor::Editor::run()?;
        return Ok(());
    }

    // 如果没有指定用户和命令，显示默认用户信息 + 帮助
    if params.user.is_none() && params.command.is_none() {
        // 加载默认账户信息并显示
        if let Ok(account) = get_account_config(None) {
            println!("user: \"{}\"", account.user);
            println!("description: \"{}\"", account.description.as_deref().unwrap_or(""));
            println!("access_key: \"{}\"", account.access_key);
            println!("secret_key: \"{}\"", account.secret_key);
            println!("url: \"{}\"", account.url);
            println!();
        }
        // 显示帮助信息
        use clap::Parser;
        let _ = <Params as Parser>::parse_from(["s3cli", "--help"]);
        return Ok(());
    }

    // 处理其他命令：需要确定用户
    let account = get_account_config(params.user.as_deref())?;

    // 如果没有指定命令，显示默认用户信息
    let Some(command) = params.command else {
        println!("user: \"{}\"", account.user);
        println!("description: \"{}\"", account.description.as_deref().unwrap_or(""));
        println!("access_key: \"{}\"", account.access_key);
        println!("secret_key: \"{}\"", account.secret_key);
        println!("url: \"{}\"", account.url);
        println!();
        println!("使用 's3cli --help' 查看可用命令");
        return Ok(());
    };

    // 创建 S3 客户端
    let s3_client = S3Client::new(&account.access_key, &account.secret_key, &account.url).await;

    match command {
        SubCommand::Show | SubCommand::Config => {
            // 已经在上面处理了
        }
        SubCommand::Mb { bucket } => {
            let bucket_name = parse_bucket_url(&bucket)?;
            s3_client.create_bucket(&bucket_name).await?;
            println!("Bucket s3://{} created successfully.", bucket_name);
        }
        SubCommand::Rb { bucket } => {
            let bucket_name = parse_bucket_url(&bucket)?;
            s3_client.delete_bucket(&bucket_name).await?;
            println!("Bucket s3://{} deleted successfully.", bucket_name);
        }
        SubCommand::Put {
            local_file,
            bucket,
            key,
        } => {
            let bucket_name = parse_bucket_url(&bucket)?;
            // 如果没有指定 key，使用本地文件路径作为 key
            let key = key.unwrap_or_else(|| local_file.clone());
            let file_size = tokio::fs::metadata(&local_file).await?.len();
            if file_size > CHUNK_SIZE as u64 {
                s3_client.upload_file_multipart(&bucket_name, &local_file, &key).await?;
            } else {
                s3_client.upload_file(&bucket_name, &local_file, &key).await?;
            }
            // 构建完整访问 URL
            let access_url = format!("{}/{}/{}", account.url.trim_end_matches('/'), bucket_name, key);
            println!("uploaded successfully: {}", access_url);
        }
        SubCommand::Putr {
            local_dir,
            bucket,
            prefix,
        } => {
            let bucket_name = parse_bucket_url(&bucket)?;
            let prefix_str = prefix.as_deref().unwrap_or("");
            s3_client
                .upload_dir_concurrent(&bucket_name, &local_dir, prefix_str)
                .await?;
            // 构建完整访问 URL
            let access_url = format!("{}/{}/{}", account.url.trim_end_matches('/'), bucket_name, prefix_str);
            println!("uploaded successfully: {}", access_url);
        }
        SubCommand::Get {
            bucket,
            key,
            local_file,
        } => {
            let bucket_name = parse_bucket_url(&bucket)?;
            s3_client.download_file(&bucket_name, &key, &local_file).await?;
            println!(
                "key s3://{}/{} -> {} downloaded successfully.",
                bucket_name, key, local_file
            );
        }
        SubCommand::Getr {
            bucket,
            prefix,
            local_dir,
        } => {
            let bucket_name = parse_bucket_url(&bucket)?;
            s3_client
                .download_dir_concurrent(&bucket_name, prefix.as_deref().unwrap_or(""), &local_dir)
                .await?;
            println!(
                "Directory s3://{}/{} -> {} downloaded successfully.",
                bucket_name,
                prefix.as_deref().unwrap_or(""),
                local_dir
            );
        }
        SubCommand::Ls { bucket, prefix } => match bucket {
            Some(bucket) => {
                let bucket_name = parse_bucket_url(&bucket)?;
                // 获取对象列表并以 s3cmd 格式显示
                let objects = s3_client
                    .list_objects_with_info(&bucket_name, prefix.as_deref().unwrap_or(""))
                    .await?;
                print_objects_s3cmd_format(&account, &objects);
            }
            None => {
                let buckets = s3_client.list_buckets().await?;
                for bucket in buckets {
                    println!("s3://{}", bucket);
                }
            }
        },
        SubCommand::Cp {
            src_bucket,
            src_key,
            dst_bucket,
            dst_key,
        } => {
            let src_bucket_name = parse_bucket_url(&src_bucket)?;
            let dst_bucket_name = parse_bucket_url(&dst_bucket)?;
            s3_client
                .copy_object(&src_bucket_name, &src_key, &dst_bucket_name, &dst_key)
                .await?;
            println!(
                "Object copied successfully from s3://{}/{} -> s3://{}/{}",
                src_bucket_name, src_key, dst_bucket_name, dst_key
            );
        }
        SubCommand::Mv {
            src_bucket,
            src_key,
            dst_bucket,
            dst_key,
        } => {
            let src_bucket_name = parse_bucket_url(&src_bucket)?;
            let dst_bucket_name = parse_bucket_url(&dst_bucket)?;
            s3_client
                .move_object(&src_bucket_name, &src_key, &dst_bucket_name, &dst_key)
                .await?;
            println!(
                "Object moved successfully from s3://{}/{} -> s3://{}/{}",
                src_bucket_name, src_key, dst_bucket_name, dst_key
            );
        }
        SubCommand::Del { bucket, key } => {
            let bucket_name = parse_bucket_url(&bucket)?;
            s3_client.delete_object(&bucket_name, &key).await?;
            println!("Object s3://{}/{} deleted successfully.", bucket_name, key);
        }
        SubCommand::Delr { bucket, prefix } => {
            let bucket_name = parse_bucket_url(&bucket)?;
            s3_client
                .delete_object_with_prefix(&bucket_name, prefix.as_deref().unwrap_or(""))
                .await?;
            println!(
                "Objects with prefix s3://{}/{} deleted successfully.",
                bucket_name,
                prefix.as_deref().unwrap_or("")
            );
        }
        SubCommand::Info { bucket, key } => {
            let bucket_name = parse_bucket_url(&bucket)?;
            // 显示用户信息
            println!("user: \"{}\"", account.user);
            println!("description: \"{}\"", account.description.as_deref().unwrap_or(""));
            println!("access_key: \"{}\"", account.access_key);
            println!("secret_key: \"{}\"", account.secret_key);
            println!("url: \"{}\"", account.url);
            println!();
            
            match key {
                Some(key) => {
                    let obj_info = s3_client.display_object_info(&bucket_name, &key).await?;
                    println!("{}", obj_info);
                }
                None => {
                    let bucket_info = s3_client.display_bucket_info(&bucket_name).await?;
                    println!("{}", bucket_info);
                }
            }
        }
        SubCommand::Mime {
            bucket,
            prefix,
            mime,
        } => {
            let bucket_name = parse_bucket_url(&bucket)?;
            s3_client
                .set_prefix_mime(&bucket_name, &prefix.unwrap_or_default(), &mime)
                .await?;
            println!("MIME type set to {} .", mime);
        }
        SubCommand::Signurl {
            bucket,
            key,
            expires,
        } => {
            let bucket_name = parse_bucket_url(&bucket)?;
            let url = s3_client.sign_url(&bucket_name, &key, expires).await?;
            println!("Signed URL: {}", url);
        }
        SubCommand::Url { bucket, key } => {
            let bucket_name = parse_bucket_url(&bucket)?;
            let url = s3_client.get_object_raw_url(&bucket_name, &key).await?;
            println!("Raw URL: {}", url);
        }
        SubCommand::Public { bucket_url } => {
            // 需要 admin 权限
            let bucket = parse_bucket_url(&bucket_url)?;
            s3_client.set_bucket_public(&bucket).await?;

            // 显示更新后的信息
            let info = s3_client.display_bucket_info(&bucket).await?;
            println!("{}", info);
        }
        SubCommand::Expire { bucket_url, days } => {
            // 需要 admin 权限
            // expire 只接受 bucket 名称，不接受路径
            let bucket = parse_bucket_url(&bucket_url)?;
            
            // 检查 bucket_url 是否包含路径
            let path_part = bucket_url.strip_prefix("s3://").unwrap_or(&bucket_url);
            if path_part.contains('/') {
                return Err(anyhow!("expire 命令只接受 bucket 名称，不接受路径\n用法：s3cli expire s3://bucket 90"));
            }

            // 设置整个桶的对象在指定天数后过期
            s3_client.set_bucket_lifecycle(&bucket, "", days).await?;

            // 显示更新后的信息
            let info = s3_client.display_bucket_info(&bucket).await?;
            println!("{}", info);
        }
        SubCommand::Lifecycle { days, bucket_path } => {
            // 需要 admin 权限
            let bucket_path_clean = bucket_path.strip_prefix("s3://").ok_or_else(|| anyhow!("Invalid bucket path, should be like s3://bucket/prefix"))?;

            // 分离 bucket 和 prefix
            let parts: Vec<&str> = bucket_path_clean.splitn(2, '/').collect();
            let bucket = parts[0];
            let prefix = if parts.len() > 1 { parts[1] } else { "" };

            s3_client.set_bucket_lifecycle(bucket, prefix, days).await?;

            // 显示更新后的信息
            let info = s3_client.display_bucket_info(bucket).await?;
            println!("{}", info);
        }
    }

    Ok(())
}
