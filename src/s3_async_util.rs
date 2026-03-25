use anyhow::anyhow;
use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::MetadataDirective;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::types::{Delete, ObjectIdentifier}; // 用于批量删除
use aws_sdk_s3::Client;
use aws_sdk_s3::Config;
use futures::future::try_join_all;
use log::{info, warn};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Semaphore;
use urlencoding::encode;
use walkdir::WalkDir;

// 引入配置模块
use crate::s3_cfg_util::CephKeysConfig;

// 默认区
pub const DEFAULT_REGION: &str = "us-east-1";
// 最大并发数
pub const MAX_CONCURRENT_TASKS: usize = 50;
// 分片大小，10 MB
pub const CHUNK_SIZE: usize = 10 * 1024 * 1024;

/// 对象信息（用于 ls 命令的格式化输出）
#[derive(Clone, Debug)]
pub struct ObjectInfo {
    pub path: String,
    pub size: u64,
    pub last_modified: Option<SystemTime>,
    pub is_dir: bool,
}

#[derive(Clone, Debug)]
pub struct S3Client {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub client: aws_sdk_s3::Client,
}

impl S3Client {
    /// 创建 S3 客户端
    pub async fn new(access_key: &str, secret_key: &str, endpoint: &str) -> Self {
        let credentials = Credentials::new(access_key, secret_key, None, None, "env");
        let s3_config = Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(credentials)
            .region(Region::new(DEFAULT_REGION))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_config);
        info!("S3 client created successfully.");
        Self {
            client,
            endpoint: endpoint.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
        }
    }
    /// 创建 S3 客户端
    pub async fn init() -> Self {
        let access_key = std::env::var("s3_access_key").unwrap();
        let secret_key = std::env::var("s3_secret_key").unwrap();
        let endpoint = std::env::var("s3_endpoint").unwrap();
        let credentials = Credentials::new(access_key.clone(), secret_key.clone(), None, None, "env");
        let s3_config = Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(credentials)
            .region(Region::new(DEFAULT_REGION))
            .endpoint_url(endpoint.clone())
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_config);
        info!("S3 client created successfully.");
        Self {
            client,
            endpoint,
            access_key,
            secret_key,
        }
    }

    /// 创建存储桶
    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        let client = self.client.clone();
        client.create_bucket().bucket(bucket).send().await?;
        info!("Bucket {} created successfully.", bucket);
        Ok(())
    }

    /// 获取存储桶列表
    pub async fn list_buckets(&self) -> Result<Vec<String>> {
        let client = self.client.clone();
        let resp = client.list_buckets().send().await?;
        let buckets: Vec<String> = resp
            .buckets()
            .iter()
            .map(|b| b.name.as_ref().unwrap_or(&"[Unknown]".to_string()).to_string())
            .collect();
        Ok(buckets)
    }

    /// 显示桶列表
    pub async fn display_buckets(&self) -> Result<()> {
        let buckets = self.list_buckets().await?;
        let formatted_buckets: String = buckets.into_iter().map(|b| format!("{}", b)).collect::<Vec<_>>().join("\n");
        info!("Bucket list :\n{}", formatted_buckets);
        Ok(())
    }

    /// 删除存储桶
    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        let client = self.client.clone();
        client.delete_bucket().bucket(bucket).send().await?;
        info!("Bucket {} deleted successfully.", bucket);
        Ok(())
    }

    /// 上传文件
    /// 比如：s3://tmp/python-logs/timing_20251121_180455.log  对应的key应该为python-logs/timing_20251121_180455.log
    pub async fn upload_file(&self, bucket: &str, local_file: &str, key: &str) -> Result<()> {
        let client = self.client.clone();
        let body = tokio::fs::read(local_file).await?;
        let body_into = ByteStream::from(body);
        client.put_object().bucket(bucket).key(key).body(body_into).send().await?;
        info!("File {} uploaded successfully.", key);
        Ok(())
    }

    /// 上传大文件
    pub async fn upload_file_multipart(&self, bucket: &str, local_file: &str, key: &str) -> Result<()> {
        info!("Start Upload BigFile , {} -> s3://{}/{}", local_file, bucket, key);
        let client = self.client.clone();
        let file_path = Path::new(local_file);
        if !file_path.exists() {
            return Err(anyhow!("File not found: {}", local_file));
        }

        // ==========================================
        // 步骤 1: 初始化分片上传 (Create Multipart Upload)
        // ==========================================
        let create_resp = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .context("Failed to create multipart upload")?;
        let upload_id = create_resp.upload_id().ok_or(anyhow!("No upload_id found"))?;
        info!("Multipart upload ID: {}", upload_id);

        // ==========================================
        // 步骤 2: 循环读取并上传分片 (Upload Parts)
        // ==========================================
        let mut file = File::open(file_path).await?;
        let mut part_number = 1;
        let mut completed_parts = Vec::new();

        // 你的 10MB Buffer
        let mut buffer = vec![0u8; CHUNK_SIZE];

        loop {
            let mut offset = 0;
            while offset < CHUNK_SIZE {
                let n = file.read(&mut buffer[offset..]).await?;
                if n == 0 {
                    break;
                }
                offset += n;
            }
            if offset == 0 {
                break;
            }

            let bytes_read = offset; // 这里就是实际读到的总大小（通常是 10MB，最后一块可能少于 10MB）
            let data_chunk = &buffer[0..bytes_read];
            // 注意：如果不 clone，直接用 ByteStream::from(Vec)，这会消耗 Vec 所有权。
            // 由于 buffer 在循环外复用，这里必须 copy 数据。
            // data_chunk.to_vec() 已经做了拷贝。
            let stream = ByteStream::from(data_chunk.to_vec());
            println!("Uploading part {} ({} bytes)...", part_number, bytes_read);
            let part_resp = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id) // 注意：这里通常不需要 clone，传引用即可，看具体 SDK 版本
                .part_number(part_number)
                .body(stream)
                .send()
                .await;

            let part_resp = match part_resp {
                Ok(resp) => resp,
                Err(e) => {
                    return Err(anyhow!("Failed to upload part {}: {}", part_number, e));
                }
            };

            let etag = part_resp.e_tag().ok_or(anyhow!("No ETag for part {}", part_number))?;
            completed_parts.push(CompletedPart::builder().e_tag(etag).part_number(part_number).build());
            part_number += 1;
        }

        // ==========================================
        // 步骤 3: 完成上传 (Complete Multipart Upload)
        // ==========================================
        if completed_parts.is_empty() {
            // 如果文件是空的，multipart 会失败吗？
            // S3 允许空文件的 multipart，但也需要走 complete
        }

        let completed_multipart_upload = CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build();

        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed_multipart_upload)
            .send()
            .await
            .context("Failed to complete multipart upload")?;

        info!("successfully Upload BigFile , {} -> s3://{}/{}", local_file, bucket, key);
        Ok(())
    }

    /// 上传字节数组
    pub async fn upload_data(&self, bucket: &str, data: Vec<u8>, key: &str) -> Result<(), anyhow::Error> {
        let client = self.client.clone();
        let body_into = ByteStream::from(data);
        client.put_object().bucket(bucket).key(key).body(body_into).send().await?;
        info!("Data uploaded successfully.");
        Ok(())
    }

    /// 设置对象的MIME，例如 "image/jpeg" 或 "image/png"
    pub async fn set_prefix_mime(&self, bucket: &str, prefix: &str, mime_type: &str) -> anyhow::Result<()> {
        let client = self.client.clone();
        let keys = self.list_objects_page(bucket, prefix).await?;
        for key in keys {
            let copy_source = format!("{}/{}", bucket, key);
            client
                .copy_object()
                .bucket(bucket)
                .key(key)
                .copy_source(copy_source)
                .metadata_directive(MetadataDirective::Replace)
                .content_type(mime_type)
                .send()
                .await?;
        }
        info!("Prefix {} MIME type updated to {}.", prefix, mime_type);
        Ok(())
    }

    /// 显示对象的信息
    pub async fn display_object_info(&self, bucket: &str, key: &str) -> anyhow::Result<String> {
        let client = self.client.clone();
        // 1. 获取基础元数据 (HeadObject)
        let head = client.head_object().bucket(bucket).key(key).send().await?;

        // 2. 获取权限信息 (GetObjectAcl) - s3cmd 通常会显示这个
        let acl = client.get_object_acl().bucket(bucket).key(key).send().await?;

        // 3. 组装字符串
        let mut print_info = String::new();
        print_info.push_str(&format!("s3://{}/{}", bucket, key));
        print_info.push_str(&format!("   File size: {}", head.content_length().unwrap_or(0)));
        print_info.push_str(&format!("   Last mod:  {}", head.last_modified().map(|t| t.to_string()).unwrap_or_default()));
        print_info.push_str(&format!("   MIME type: {}", head.content_type().unwrap_or("unknown")));
        print_info.push_str(&format!("   ETag:      {}", head.e_tag().unwrap_or_default()));

        // 打印自定义元数据
        if let Some(metadata) = head.metadata() {
            for (k, v) in metadata {
                print_info.push_str(&format!("   Metadata:  x-amz-meta-{}: {}", k, v));
            }
        }

        // 打印所有者信息
        if let Some(owner) = acl.owner() {
            print_info.push_str(&format!("   Owner:     {}", owner.display_name().unwrap_or("unknown")));
        }

        Ok(print_info)
    }

    /// 显示桶的信息
    pub async fn display_bucket_info(&self, bucket: &str) -> anyhow::Result<String> {
        let client = self.client.clone();

        // 1. 检查桶是否存在及权限 (HeadBucket)
        client.head_bucket().bucket(bucket).send().await?;
        // 2. 获取桶的位置 (GetBucketLocation)
        let location_resp = client.get_bucket_location().bucket(bucket).send().await?;
        let region = location_resp.location_constraint().map(|r| r.as_str()).unwrap_or("us-east-1");
        // 3. 获取桶的 ACL
        let acl = client.get_bucket_acl().bucket(bucket).send().await?;
        let mut print_info = String::new();

        // 4. 组装string
        print_info.push_str(&format!("s3://{}", bucket));
        print_info.push_str(&format!("   Location:  {}", region));
        if let Some(owner) = acl.owner() {
            print_info.push_str(&format!("   Owner:     {}", owner.display_name().unwrap_or("unknown")));
        }
        print_info.push_str(&format!("   Payer:     BucketOwner"));

        info!("{}", print_info);
        Ok(print_info)
    }

    /// 下载文件
    /// * bucket: 存储桶名称
    /// * key: 对象键（即存储在S3中的文件路径），比如：python-logs/timing_20251121_180455.log
    /// * download_file_path: 下载文件路径
    pub async fn download_file(&self, bucket: &str, key: &str, dest_path: &str) -> Result<()> {
        let client = self.client.clone();
        let resp = client.get_object().bucket(bucket).key(key).send().await?;
        let p = Path::new(dest_path).parent().ok_or(anyhow::format_err!("Parent directory not found"))?;
        tokio::fs::create_dir_all(p).await.context("Failed to create directory")?;
        let file = File::create(dest_path).await?;
        let mut writer = BufWriter::with_capacity(4 * 1024 * 1024, file);
        let mut stream = resp.body;
        while let Some(bytes) = stream.try_next().await? {
            writer.write_all(&bytes).await?;
        }
        writer.flush().await?;
        info!("File {} downloaded successfully.", key);
        Ok(())
    }

    /// 复制对象
    pub async fn copy_object(&self, src_bucket: &str, src_key: &str, dest_bucket: &str, dest_key: &str) -> Result<()> {
        let client = self.client.clone();
        // Copy Source 的格式通常是 "bucket/key"，且需要 URL 编码
        // 这里简单处理，如果 key 包含特殊字符建议使用 urlencoding crate
        // 安全做法：对 bucket 和 key 分别编码，然后拼接
        let encoded_bucket = encode(src_bucket);
        let encoded_key = encode(src_key);
        let copy_source = format!("{}/{}", encoded_bucket, encoded_key);
        client
            .copy_object()
            .copy_source(copy_source)
            .bucket(dest_bucket)
            .key(dest_key)
            .send()
            .await
            .context("Failed to copy object")?;
        info!("Object copied successfully from {}/{ } -> {}/{ }", src_bucket, src_key, dest_bucket, dest_key);
        Ok(())
    }

    /// 删除对象
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let client = self.client.clone();
        client.delete_object().bucket(bucket).key(key).send().await?;
        info!("Object deleted successfully from {}/{ }", bucket, key);
        Ok(())
    }

    /// 移动对象
    pub async fn move_object(&self, src_bucket: &str, src_key: &str, dest_bucket: &str, dest_key: &str) -> Result<()> {
        let client = self.client.clone();
        // Copy Source 的格式通常是 "bucket/key"，且需要 URL 编码
        // 这里简单处理，如果 key 包含特殊字符建议使用 urlencoding crate
        // 安全做法：对 bucket 和 key 分别编码，然后拼接
        let encoded_bucket = encode(src_bucket);
        let encoded_key = encode(src_key);
        let copy_source = format!("{}/{}", encoded_bucket, encoded_key);
        client
            .copy_object()
            .copy_source(copy_source)
            .bucket(dest_bucket)
            .key(dest_key)
            .send()
            .await
            .context("Failed to copy object")?;
        client.delete_object().bucket(src_bucket).key(src_key).send().await?;
        info!("Object moved successfully from {}/{ } -> {}/{ }", src_bucket, src_key, dest_bucket, dest_key);
        Ok(())
    }

    /// 获取对象的原生URL
    pub async fn get_object_raw_url(&self, bucket: &str, key: &str) -> anyhow::Result<String> {
        let base_url = self.endpoint.clone();
        let url = format!("{}/{}/{}", base_url, bucket, key);
        log::info!("Generated raw URL : {}", url);
        Ok(url)
    }

    /// 获取对象临时访问URL
    /// expires_in_secs 的意思是 “预签名链接的有效期（秒）
    pub async fn sign_url(&self, bucket: &str, key: &str, expires_in_secs: u64) -> Result<String> {
        let client = self.client.clone();
        // 1. 定义有效期
        let expires_in = Duration::from_secs(expires_in_secs);
        let presigning_config = PresigningConfig::builder()
            .expires_in(expires_in)
            .build()
            .context("Failed to create presigning config")?;

        // 2. 生成预签名请求 (Get 操作)
        let presigned_request = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .presigned(presigning_config)
            .await
            .context("Failed to generate presigned URL")?;

        // 3. 提取 URL
        let url = presigned_request.uri().to_string();
        log::info!("Generated signed URL : {}", url);
        Ok(url)
    }

    /// 获取对象列表，prefix: 指定前缀路径
    pub async fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let client = self.client.clone();
        let resp: Vec<String> = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .send()
            .await?
            .contents
            .unwrap_or(vec![])
            .iter()
            .map(|obj| obj.key.as_ref().unwrap().to_string().clone())
            .collect();
        Ok(resp)
    }

    // list_objects_page: 列出指定目录下的所有文件
    pub async fn list_objects_page(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let client = self.client.clone();
        let mut resp: Vec<String> = Vec::new();
        let mut paginator = client.list_objects_v2().bucket(bucket).prefix(prefix).into_paginator().send();
        while let Some(page) = paginator.next().await {
            let page = page?;
            for obj in page.contents() {
                let key = obj.key().unwrap_or_default();
                if key.ends_with('/') {
                    continue;
                }
                resp.push(key.to_string());
            }
        }
        Ok(resp)
    }

    /// 列出对象及其详细信息（用于 ls 命令的格式化输出）
    pub async fn list_objects_with_info(&self, bucket: &str, prefix: &str) -> Result<Vec<ObjectInfo>> {
        let client = self.client.clone();
        let mut resp: Vec<ObjectInfo> = Vec::new();
        let mut paginator = client.list_objects_v2().bucket(bucket).prefix(prefix).into_paginator().send();
        
        while let Some(page) = paginator.next().await {
            let page = page?;
            
            // 处理公共前缀（目录）
            for common_prefix in page.common_prefixes() {
                if let Some(prefix_str) = common_prefix.prefix() {
                    resp.push(ObjectInfo {
                        path: format!("s3://{}/{}", bucket, prefix_str),
                        size: 0,
                        last_modified: None,
                        is_dir: true,
                    });
                }
            }
            
            // 处理对象
            for obj in page.contents() {
                let key = obj.key().unwrap_or_default();
                if key.ends_with('/') {
                    continue;
                }
                
                // 转换大小
                let size = obj.size().unwrap_or(0) as u64;
                
                // 转换时间
                let last_modified = obj.last_modified()
                    .and_then(|dt| dt.to_millis().ok())
                    .map(|millis| UNIX_EPOCH + Duration::from_millis(millis as u64));
                
                resp.push(ObjectInfo {
                    path: format!("s3://{}/{}", bucket, key),
                    size,
                    last_modified,
                    is_dir: false,
                });
            }
        }
        
        // 排序：目录在前，文件在后，按路径排序
        resp.sort_by(|a, b| {
            if a.is_dir && !b.is_dir {
                std::cmp::Ordering::Less
            } else if !a.is_dir && b.is_dir {
                std::cmp::Ordering::Greater
            } else {
                a.path.cmp(&b.path)
            }
        });
        
        Ok(resp)
    }

    /// 显示对象列表
    pub async fn display_objects(&self, bucket: &str, prefix: &str) -> Result<()> {
        let objects = self.list_objects_page(bucket, prefix).await?;
        let formatted_objects: String = objects.into_iter().map(|b| format!("{}", b)).collect::<Vec<_>>().join("\n");
        info!("Object list:\n{}", formatted_objects);
        Ok(())
    }

    /// 上传目录
    pub async fn upload_dir(&self, bucket: &str, local_dir: &str, prefix: &str) -> Result<()> {
        let client = self.client.clone();
        info!("Starting upload , {} -> s3://{}/{}", local_dir, bucket, prefix,);

        // 使用 WalkDir 同步遍历目录 (WalkDir 本身不是异步的，但在循环里调用异步上传即可)
        for entry in WalkDir::new(local_dir).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();
            // 只上传文件，忽略目录本身
            if path.is_dir() {
                continue;
            }
            // 计算相对路径：例如 local_dir="/tmp/data", path="/tmp/data/sub/a.txt"
            // relative_path 就是 "sub/a.txt"
            let relative_path = path.strip_prefix(local_dir).context("Failed to strip prefix")?;

            // 构造 S3 Key: prefix + relative_path
            // 注意：Rust 的 Path 在 Windows 下是 "\", S3 需要 "/", 需要转换
            let relative_str = relative_path.to_str().ok_or(anyhow!("Invalid path encoding"))?;

            // 简单处理路径分隔符 (将 \ 替换为 /)
            let normalized_relative = relative_str.replace("\\", "/");

            // 拼接最终 Key (处理 prefix 结尾是否带 / 的情况，避免双斜杠)
            let key = if prefix.ends_with('/') {
                format!("{}{}", prefix, normalized_relative)
            } else if prefix.is_empty() {
                normalized_relative
            } else {
                format!("{}/{}", prefix, normalized_relative)
            };

            info!("Uploading: {:?} -> {}", path, key);

            // 读取文件内容 (使用 read 读入内存以避免 Ceph 签名兼容性问题)
            let data = fs::read(path).await?;
            let body = ByteStream::from(data);

            client
                .put_object()
                .bucket(bucket)
                .key(&key)
                .body(body)
                .send()
                .await
                .context(format!("Failed to upload {}", key))?;
        }
        info!("upload successfully , {} -> s3://{}/{}", local_dir, bucket, prefix,);
        Ok(())
    }

    /// 并发上传目录
    pub async fn upload_dir_concurrent(&self, bucket: &str, local_dir: &str, prefix: &str) -> Result<()> {
        info!("Starting upload , {} -> s3://{}/{}", local_dir, bucket, prefix,);
        let client = self.client.clone();
        // 1. 收集所有需要上传的文件路径
        let mut tasks = Vec::new();
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_TASKS));
        let client = client.clone();
        let bucket = bucket.to_string();
        let prefix = prefix.to_string();
        let local_base_dir = local_dir.to_string();

        // 遍历目录 (同步操作，很快)
        for entry in WalkDir::new(&local_base_dir).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path().to_path_buf();
            if !path.is_file() {
                continue;
            }

            // 准备 Clone 的变量移动到闭包中
            let permit = semaphore.clone().acquire_owned().await?;
            let client = client.clone();
            let bucket = bucket.clone();
            let prefix = prefix.clone();
            let local_base_str = local_base_dir.clone();

            // 2. Spawn 任务
            let task = tokio::spawn(async move {
                // 确保任务结束时释放信号量 (permit 在这里 drop)
                let _permit = permit;

                // 计算 Key (处理 Windows 路径)

                // 计算相对路径：例如 local_base_dir="/tmp/data", path="/tmp/data/sub/a.txt"
                // relative_path 就是 "sub/a.txt"
                let relative_path = path.strip_prefix(local_base_str).context("Failed to strip prefix")?;

                // 构造 S3 Key: prefix + relative_path
                // 注意：Rust 的 Path 在 Windows 下是 "\", S3 需要 "/", 需要转换
                let relative_str = relative_path.to_str().ok_or(anyhow!("Invalid path encoding"))?;

                // 简单处理路径分隔符 (将 \ 替换为 /)
                let normalized_relative = relative_str.replace("\\", "/");

                // 拼接最终 Key (处理 prefix 结尾是否带 / 的情况，避免双斜杠)
                let key = if prefix.ends_with('/') {
                    format!("{}{}", prefix, normalized_relative)
                } else if prefix.is_empty() {
                    normalized_relative
                } else {
                    format!("{}/{}", prefix, normalized_relative)
                };

                info!("Uploading: {:?} -> {}", path, key);

                // 读取并上传 (内存方式，兼容 Ceph)
                let data = fs::read(&path).await.context(format!("Read failed: {:?}", path))?;
                let body = ByteStream::from(data);
                info!("Uploading: {:?} -> {}", path, key);

                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(body)
                    .send()
                    .await
                    .context(format!("Upload failed: {}", key))?;

                Ok::<(), anyhow::Error>(())
            });

            tasks.push(task);
        }

        // 3. 等待所有任务完成
        // try_join_all 会等待所有 handle 完成，如果 task 内部 panic 或返回 Err，这里会捕获
        let results = try_join_all(tasks).await?;

        // 检查是否有业务逻辑错误
        for res in results {
            res?;
        }
        info!("upload successfully , {} -> s3://{}/{}", local_base_dir, bucket, prefix,);
        Ok(())
    }

    /// 下载目录
    pub async fn download_dir(&self, bucket: &str, prefix: &str, local_base_dir: &str) -> Result<()> {
        info!("Starting download ,s3://{}/{} ->  {}", bucket, prefix, local_base_dir);
        let client = self.client.clone();
        let prefix_with_slash = if prefix.is_empty() || prefix.ends_with('/') { prefix.to_string() } else { format!("{}/", prefix) };
        let mut paginator = client.list_objects_v2().bucket(bucket).prefix(&prefix_with_slash).into_paginator().send();
        while let Some(page) = paginator.next().await {
            let page = page.context("Failed to get list page")?;
            // 遍历页面中的每个对象
            for obj in page.contents() {
                let key = obj.key().unwrap_or_default();
                // 跳过目录占位符 (以 / 结尾的对象)
                if key.ends_with('/') {
                    continue;
                }
                // 构造本地绝对路径
                // S3 Key: "data/logs/2023.log" -> Local: "/tmp/backup/data/logs/2023.log"
                let relative_key = key.strip_prefix(&prefix_with_slash).unwrap_or(&key).trim_start_matches('/').to_string();
                let local_path = Path::new(local_base_dir).join(relative_key);
                // 确保父目录存在
                if let Some(parent) = local_path.parent() {
                    fs::create_dir_all(parent).await?;
                }
                // 下载并写入文件
                info!("Downloading: {} -> {:?}", key, local_path);
                let resp = client.get_object().bucket(bucket).key(key).send().await?;
                let mut file = fs::File::create(&local_path).await?;
                // 流式写入本地磁盘
                let mut content = resp.body;
                while let Some(bytes) = content.try_next().await? {
                    file.write_all(&bytes).await?;
                }
            }
        }

        info!("download successfully  ,s3://{}/{} ->  {}", bucket, prefix, local_base_dir);

        Ok(())
    }

    pub fn get_relative_path(key: &str, prefix: &str) -> String {
        let prefix_with_slash = if prefix.is_empty() || prefix.ends_with('/') { prefix.to_string() } else { format!("{}/", prefix) };
        key.strip_prefix(&prefix_with_slash)
            .unwrap_or(key) // 如果不匹配，返回原样
            .trim_start_matches('/') // 去掉开头的斜杠
            .to_string()
    }

    /// 并发下载目录
    pub async fn download_dir_concurrent(&self, bucket: &str, prefix: &str, local_base_dir: &str) -> Result<()> {
        info!("Starting download ,s3://{}/{} ->  {}", bucket, prefix, local_base_dir);
        let client = self.client.clone();
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_TASKS));
        let mut tasks = Vec::new();
        let prefix_with_slash = if prefix.is_empty() || prefix.ends_with('/') { prefix.to_string() } else { format!("{}/", prefix) };
        let mut paginator = client.list_objects_v2().bucket(bucket).prefix(&prefix_with_slash).into_paginator().send();
        while let Some(page) = paginator.next().await {
            let page = page?;
            for obj in page.contents() {
                let key = obj.key().unwrap_or_default().to_string();
                if key.ends_with('/') {
                    continue;
                }
                let permit = semaphore.clone().acquire_owned().await?;
                let client = client.clone();
                let bucket = bucket.to_string();
                let local_base_dir = local_base_dir.to_string();
                let prefix_with_slash = prefix_with_slash.clone();
                let task = tokio::spawn(async move {
                    let _permit = permit;
                    let relative_key = key.strip_prefix(&prefix_with_slash).unwrap_or(&key).trim_start_matches('/').to_string();
                    let local_path = Path::new(&local_base_dir).join(&relative_key);
                    if let Some(parent) = local_path.parent() {
                        fs::create_dir_all(parent).await?;
                    }

                    // 下载流
                    let resp = client.get_object().bucket(&bucket).key(&key).send().await?;
                    let mut file = fs::File::create(&local_path).await?;
                    let mut content = resp.body;
                    info!("Downloading: {} -> {:?}", key, local_path);

                    // 写入磁盘
                    while let Some(bytes) = content.try_next().await? {
                        file.write_all(&bytes).await?;
                    }

                    Ok::<(), anyhow::Error>(())
                });
                tasks.push(task);
            }
        }

        // 3. 等待结果
        let results = try_join_all(tasks).await?;
        for res in results {
            res?;
        }

        info!("download successfully  ,s3://{}/{} ->  {}", bucket, prefix, local_base_dir);
        Ok(())
    }

    // 批量删除 (Delete Dir Bulk) - 最佳实践
    // 注意：S3 删除不是并发单删，而是并发“批量删”。
    // 一次 delete_objects 请求最多可以删 1000 个 Key。
    pub async fn delete_object_with_prefix(&self, bucket: &str, prefix: &str) -> Result<()> {
        info!("start delete object: s3://{}/{}", bucket, prefix);
        let client = self.client.clone();
        let mut paginator = client.list_objects_v2().bucket(bucket).prefix(prefix).into_paginator().send();

        let mut batch_keys = Vec::new();

        while let Some(page) = paginator.next().await {
            let page = page?;

            for obj in page.contents() {
                let key = obj.key().unwrap_or_default().to_string();

                // 构造 ObjectIdentifier
                batch_keys.push(ObjectIdentifier::builder().key(key).build()?);

                // 如果积攒够了 1000 个，发送一次删除请求
                if batch_keys.len() >= 1000 {
                    self.flush_delete_batch(bucket, &batch_keys).await?;
                    batch_keys.clear();
                }
            }
        }

        // 处理剩余不到 1000 个的文件
        if !batch_keys.is_empty() {
            self.flush_delete_batch(bucket, &batch_keys).await?;
        }

        info!("All objects deleted. s3://{}/{}", bucket, prefix);
        Ok(())
    }

    // 辅助函数：执行真正的批量删除请求
    async fn flush_delete_batch(&self, bucket: &str, objects: &[ObjectIdentifier]) -> Result<()> {
        let client = self.client.clone();
        if objects.is_empty() {
            return Ok(());
        }
        info!("Deleting batch of {} files...", objects.len());
        let delete_request = Delete::builder().set_objects(Some(objects.to_vec())).build()?; // 新版 SDK 可能不需要 context
        let resp = client.delete_objects().bucket(bucket).delete(delete_request).send().await?;
        for err in resp.errors() {
            warn!("Failed to delete {}: {}", err.key().unwrap_or("?"), err.message().unwrap_or("?"));
        }

        Ok(())
    }

    // ============================================
    // Bucket Policy 相关方法
    // ============================================

    /// 获取桶的 Policy
    pub async fn get_bucket_policy(&self, bucket: &str) -> Result<Option<String>> {
        let client = self.client.clone();
        match client.get_bucket_policy().bucket(bucket).send().await {
            Ok(resp) => Ok(resp.policy().map(|s| s.to_string())),
            Err(e) => {
                // 如果没有 policy，AWS SDK 会返回 NoSuchBucketPolicy
                let err_str = e.to_string();
                if err_str.contains("NoSuchBucketPolicy") {
                    Ok(None)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// 设置桶的 Policy
    pub async fn set_bucket_policy(&self, bucket: &str, policy: &str) -> Result<()> {
        let client = self.client.clone();
        client.put_bucket_policy().bucket(bucket).policy(policy).send().await?;
        info!("Bucket policy set for {}", bucket);
        Ok(())
    }

    /// 删除桶的 Policy
    pub async fn delete_bucket_policy(&self, bucket: &str) -> Result<()> {
        let client = self.client.clone();
        client.delete_bucket_policy().bucket(bucket).send().await?;
        info!("Bucket policy deleted for {}", bucket);
        Ok(())
    }

    /// 设置桶为公有访问（添加公有读策略）
    pub async fn set_bucket_public(&self, bucket: &str) -> Result<()> {
        let policy = format!(
            r#"{{
    "Statement": [
        {{
            "Action": [
                "s3:GetObject",
                "s3:GetObjectAcl"
            ],
            "Effect": "Allow",
            "Principal": "*",
            "Resource": [
                "arn:aws:s3:::{0}/*"
            ],
            "Sid": "public-read"
        }}
    ],
    "Version": "2012-10-17"
}}"#,
            bucket
        );
        self.set_bucket_policy(bucket, &policy).await?;
        info!("Bucket {} set to public access", bucket);
        Ok(())
    }

    /// 授权用户访问桶
    pub async fn grant_bucket_access(&self, bucket: &str, grant_user: &str) -> Result<()> {
        // 获取被授权用户的 access_key
        let config = CephKeysConfig::load()?;
        let user_account = config.get_account(grant_user)?;
        
        let policy = format!(
            r#"{{
    "Statement": [
        {{
            "Action": [
                "s3:GetObject",
                "s3:GetObjectAcl",
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:DeleteObject"
            ],
            "Effect": "Allow",
            "Principal": {{
                "AWS": "arn:aws:iam::s3:user/{}"
            }},
            "Resource": [
                "arn:aws:s3:::{1}/*"
            ],
            "Sid": "grant-access"
        }}
    ],
    "Version": "2012-10-17"
}}"#,
            user_account.access_key, bucket
        );
        self.set_bucket_policy(bucket, &policy).await?;
        info!("Bucket {} granted access to user {}", bucket, grant_user);
        Ok(())
    }

    // ============================================
    // Bucket Lifecycle 相关方法
    // ============================================

    /// 获取桶的生命周期配置
    pub async fn get_bucket_lifecycle(&self, bucket: &str) -> Result<Option<String>> {
        let client = self.client.clone();
        match client.get_bucket_lifecycle_configuration().bucket(bucket).send().await {
            Ok(resp) => {
                // 将 LifecycleConfiguration 转换为 XML 字符串
                let rules = resp.rules();
                if rules.is_empty() {
                    Ok(None)
                } else {
                    // 构建 XML 格式的生命周期配置
                    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
                    for rule in rules {
                        xml.push_str("  <Rule>\n");
                        if let Some(id) = rule.id() {
                            xml.push_str(&format!("    <ID>{}</ID>\n", id));
                        }
                        xml.push_str(&format!("    <Status>{}</Status>\n", rule.status().as_str()));
                        if let Some(expiration) = rule.expiration() {
                            if let Some(days) = expiration.days() {
                                xml.push_str(&format!("    <Expiration><Days>{}</Days></Expiration>\n", days));
                            }
                        }
                        if let Some(filter) = rule.filter() {
                            if let Some(prefix) = filter.prefix() {
                                xml.push_str(&format!("    <Filter><Prefix>{}</Prefix></Filter>\n", prefix));
                            }
                        }
                        xml.push_str("  </Rule>\n");
                    }
                    xml.push_str("</LifecycleConfiguration>");
                    Ok(Some(xml))
                }
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("NoSuchLifecycleConfiguration") {
                    Ok(None)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// 设置桶的生命周期配置（指定前缀的对象在 N 天后过期）
    pub async fn set_bucket_lifecycle(&self, bucket: &str, prefix: &str, days: i32) -> Result<()> {
        let client = self.client.clone();

        let rule = aws_sdk_s3::types::LifecycleRule::builder()
            .id(format!("expire-{}-after-{}-days", prefix.replace("/", "-"), days))
            .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
            .filter(
                aws_sdk_s3::types::LifecycleRuleFilter::builder()
                    .prefix(prefix)
                    .build()
            )
            .expiration(
                aws_sdk_s3::types::LifecycleExpiration::builder()
                    .days(days)
                    .build()
            )
            .build()?;

        let lifecycle_config = aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
            .rules(rule)
            .build()?;

        client
            .put_bucket_lifecycle_configuration()
            .bucket(bucket)
            .lifecycle_configuration(lifecycle_config)
            .send()
            .await?;

        info!("Bucket lifecycle set for {}: prefix={} expire in {} days", bucket, prefix, days);
        Ok(())
    }

    /// 删除桶的生命周期配置
    pub async fn delete_bucket_lifecycle(&self, bucket: &str) -> Result<()> {
        let client = self.client.clone();
        client.delete_bucket_lifecycle().bucket(bucket).send().await?;
        info!("Bucket lifecycle deleted for {}", bucket);
        Ok(())
    }

    /// 设置对象过期时间（通过添加 x-delete-after 头）
    pub async fn set_object_expire(&self, bucket: &str, key: &str, days: i32) -> Result<()> {
        // 注意：这需要 Ceph RGW 支持 x-delete-after 头
        // 这里使用 copy_object 来更新对象的元数据
        let client = self.client.clone();
        let copy_source = format!("{}/{}", urlencoding::encode(bucket), urlencoding::encode(key));
        
        client
            .copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(copy_source)
            .metadata_directive(MetadataDirective::Replace)
            .metadata("delete-after", days.to_string())
            .send()
            .await?;
        
        info!("Object expire set: s3://{}/{} will expire in {} days", bucket, key, days);
        Ok(())
    }
}
