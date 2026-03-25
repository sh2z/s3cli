# s3cli

Ceph RGW 多用户命令行工具，支持交互式配置管理。

## 功能特性

- 🔐 **多用户支持**：管理多个 Ceph RGW 账户
- 🎯 **交互式配置**：友好的 TUI 界面管理账户
- 📋 **表格展示**：精美表格展示账户信息
- 🔗 **s3:// 前缀**：支持 `s3://bucket` 格式的 bucket 参数
- 🛡️ **安全保护**：禁止删除默认账户，防止误操作

## 安装

```bash
# 编译安装到 ~/.dev/bin
cargo install --path . --root ~/.dev --bin s3cli --force
```

## 配置文件

配置文件位置：`~/.config/s3cli/ceph_keys.yaml`

配置格式：
```yaml
default_account: tmp
accounts:
  - user: tmp
    url: "https://s3.example.com"
    description: "临时上传"
    access_key: "YOUR_ACCESS_KEY"
    secret_key: "YOUR_SECRET_KEY"
  - user: dev
    url: "https://s3-dev.example.com"
    description: "测试环境"
    access_key: "YOUR_ACCESS_KEY"
    secret_key: "YOUR_SECRET_KEY"
```

## 使用示例

### 基本命令

```bash
# 显示帮助
s3cli

# 显示所有账户信息（含 URL）
s3cli show

# 进入交互式配置管理
s3cli config
```

### 使用特定账户

```bash
# 指定用户执行命令
s3cli tmp ls s3://mybucket

# 使用默认账户（省略用户名）
s3cli ls s3://mybucket
```

### 桶操作

```bash
# 列出桶
s3cli tmp ls

# 列出桶中的对象
s3cli tmp ls s3://mybucket
s3cli tmp ls mybucket  # 也支持不带 s3:// 前缀

# 创建桶
s3cli tmp mb s3://newbucket

# 删除空桶
s3cli tmp rb s3://oldbucket
```

### 文件操作

```bash
# 上传文件
s3cli tmp put local.txt s3://bucket remote.txt

# 下载文件
s3cli tmp get s3://bucket/remote.txt local.txt

# 递归上传目录
s3cli tmp putr ./local/dir s3://bucket prefix/

# 递归下载目录
s3cli tmp getr s3://bucket prefix/ ./local/dir

# 删除对象
s3cli tmp del s3://bucket/file.txt

# 递归删除对象
s3cli tmp delr s3://bucket prefix/
```

### 高级操作

```bash
# 复制对象
s3cli tmp cp s3://src/file.txt s3://dst/file.txt

# 移动对象
s3cli tmp mv s3://src/file.txt s3://dst/file.txt

# 设置 MIME 类型
s3cli tmp mime s3://bucket prefix/ image/jpeg

# 生成签名 URL
s3cli tmp signurl s3://bucket/file.txt 3600

# 获取原生 URL
s3cli tmp url s3://bucket/file.txt
```

### 管理命令（需要 admin 权限）

```bash
# 设置桶为公有访问
s3cli admin public s3://bucket

# 授权用户访问桶
s3cli admin grant username s3://bucket

# 设置桶过期时间
s3cli admin expire s3://bucket 90

# 设置生命周期规则
s3cli admin lifecycle 30 s3://bucket/prefix
```

## 交互式配置管理

运行 `s3cli config` 进入配置管理界面：

```
⚙️  S3 配置管理

  添加账户
  删除账户
  设置默认账户
  查看配置
  退出
```

### 功能说明

- **添加账户**：交互式输入用户名、URL、密钥等信息
- **删除账户**：带确认提示，禁止删除默认账户
- **设置默认账户**：选择默认使用的账户
- **查看配置**：显示当前配置信息

## 安全提示

⚠️ **重要**：
- 配置文件 `~/.config/s3cli/ceph_keys.yaml` 包含敏感信息，请妥善保管
- 该文件已添加到 `.gitignore`，不会被提交到 git
- 禁止删除默认账户，防止程序无法正常工作

## 开发

```bash
# 运行
cargo run --bin s3cli -- show

# 测试
cargo test

# 编译
cargo build --release

# 格式化
cargo fmt

# 检查
cargo clippy
```

## License

MIT
