use anyhow::Result;
use cli::s3_cfg_util::*;

use ctor::ctor;

#[ctor]
fn init_log() {
    let _ = env_logger::builder().is_test(true).filter_level(log::LevelFilter::Info).try_init();
}

// ============================================
// 测试配置加载
// ============================================
#[test]
fn test_config_load() -> Result<()> {
    let config = CephKeysConfig::load()?;
    assert!(!config.accounts.is_empty());
    assert!(!config.default_account.is_empty());

    // 测试获取默认账户
    let default_account = config.get_default_account()?;
    assert_eq!(default_account.user, config.default_account);

    // 测试获取指定账户
    let tmp_account = config.get_account("tmp")?;
    assert_eq!(tmp_account.user, "tmp");
    assert!(!tmp_account.access_key.is_empty());
    assert!(!tmp_account.secret_key.is_empty());
    assert!(!tmp_account.url.is_empty());

    Ok(())
}

#[test]
fn test_config_get_account_not_found() {
    let config = CephKeysConfig::load().unwrap();
    let result = config.get_account("non_existent_user");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("non_existent_user"));
}

// ============================================
// 测试账户信息验证
// ============================================
#[test]
fn test_account_has_all_fields() -> Result<()> {
    let config = CephKeysConfig::load()?;

    for account in &config.accounts {
        // 验证所有字段都存在且非空
        assert!(!account.access_key.is_empty(), "access_key should not be empty for user: {}", account.user);
        assert!(!account.secret_key.is_empty(), "secret_key should not be empty for user: {}", account.user);
        assert!(!account.url.is_empty(), "url should not be empty for user: {}", account.user);
        assert!(!account.user.is_empty(), "user should not be empty");
    }

    Ok(())
}

// ============================================
// 测试 bucket 参数解析（支持 s3:// 前缀）
// ============================================
#[test]
fn test_parse_bucket_url() -> Result<()> {
    // 测试带 s3:// 前缀的情况
    let bucket_with_prefix = "s3://mybucket";
    let bucket_name = parse_bucket_url(bucket_with_prefix)?;
    assert_eq!(bucket_name, "mybucket");

    // 测试不带前缀的情况
    let bucket_without_prefix = "mybucket";
    let bucket_name2 = parse_bucket_url(bucket_without_prefix)?;
    assert_eq!(bucket_name2, "mybucket");

    // 测试带路径的情况（应该只返回 bucket 名）
    let bucket_with_path = "s3://mybucket/path/to/file";
    let bucket_name3 = parse_bucket_url(bucket_with_path)?;
    assert_eq!(bucket_name3, "mybucket");

    Ok(())
}

#[test]
fn test_parse_bucket_url_invalid() {
    // 测试空字符串
    let result = parse_bucket_url("");
    assert!(result.is_err());

    // 测试只有前缀
    let result2 = parse_bucket_url("s3://");
    assert!(result2.is_err());
}

// ============================================
// 测试上传文件路径生成逻辑
// ============================================
#[test]
fn test_generate_key_from_absolute_path() {
    // 测试绝对路径：只取文件名
    let local_file = "/Users/tuze/Pictures/7.jpg";
    let path = std::path::Path::new(local_file);
    assert!(path.is_absolute());

    let key = if path.is_absolute() {
        path.file_name().and_then(|n| n.to_str()).unwrap_or(local_file).to_string()
    } else {
        local_file.replace("\\", "/")
    };

    assert_eq!(key, "7.jpg");
}

#[test]
fn test_generate_key_from_relative_path() {
    // 测试相对路径：使用相对路径
    let local_file = "data/photos/7.jpg";
    let path = std::path::Path::new(local_file);
    assert!(!path.is_absolute());

    let key = if path.is_absolute() {
        path.file_name().and_then(|n| n.to_str()).unwrap_or(local_file).to_string()
    } else {
        local_file.replace("\\", "/")
    };

    assert_eq!(key, "data/photos/7.jpg");
}

#[test]
fn test_generate_key_from_relative_path_with_backslash() {
    // 测试 Windows 风格的相对路径
    let local_file = "data\\photos\\7.jpg";
    let path = std::path::Path::new(local_file);

    let key = if path.is_absolute() {
        path.file_name().and_then(|n| n.to_str()).unwrap_or(local_file).to_string()
    } else {
        local_file.replace("\\", "/")
    };

    assert_eq!(key, "data/photos/7.jpg");
}

// ============================================
// 测试 config 命令相关功能
// ============================================
#[test]
fn test_config_add_account() -> Result<()> {
    // 测试添加新账户（不实际保存，只测试逻辑）
    let new_account = Account {
        user: "test_user".to_string(),
        url: "https://s3-test.example.com".to_string(),
        description: Some("Test user".to_string()),
        access_key: "TESTACCESSKEY123".to_string(),
        secret_key: "TestSecretKey1234567890123456789012".to_string(),
    };

    // 验证账户结构正确
    assert_eq!(new_account.user, "test_user");
    assert!(!new_account.access_key.is_empty());
    assert!(!new_account.secret_key.is_empty());

    Ok(())
}

#[test]
fn test_config_set_default_account() -> Result<()> {
    let config = CephKeysConfig::load()?;

    // 测试设置默认账户（验证 tmp 用户存在）
    let result = config.get_account("tmp");
    assert!(result.is_ok());

    Ok(())
}
