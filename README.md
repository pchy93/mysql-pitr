# mysql-pitr

MySQL Point-In-Time Recovery (PITR) 工具，使用 Go 编写。基于 xtrabackup 进行全量备份，通过持续拉取 binlog 实现任意时间点恢复，备份文件存储于 S3 兼容对象存储。

## 功能特性

- **全量备份**：调用 `xtrabackup --stream=xbstream` 执行物理备份，流式压缩上传到 S3，无需双倍磁盘空间
- **持续 binlog 归档**：通过 `mysqlbinlog --read-from-remote-server` 实时拉取 binlog 并上传到 S3
- **时间点恢复**：从 S3 下载全量备份 + binlog，自动选择最近备份，恢复到指定时间点或 GTID
- **支持 S3 兼容存储**：AWS S3、MinIO、Ceph 等均可使用
- **高性能优化**：Zstd 压缩、并行上传下载、流式管道处理
- **可靠性保障**：SHA256 校验、断点续传、失败重试、备份清理

## 依赖

| 工具 | 用途 |
|------|------|
| `xtrabackup` | 全量备份与还原 |
| `mysqlbinlog` | binlog 拉取与应用 |
| `mysql` | 应用 binlog SQL |
| `mysqladmin` | MySQL 启动检测 |

## 安装

```bash
git clone https://github.com/pchy93/mysql-pitr.git
cd mysql-pitr
go build -o mysql-pitr .
```

## 快速开始

### 1. 全量备份

```bash
mysql-pitr backup \
  --host 127.0.0.1 \
  --port 3306 \
  --user root \
  --password-file /path/to/password \
  --s3-bucket your-bucket \
  --s3-endpoint http://minio:9000 \
  --s3-path-style \
  --compression zstd \
  --parallel 4
```

备份完成后输出 Backup ID，例如 `full-20260413T100000Z`。

### 2. 持续拉取 binlog（守护进程）

```bash
mysql-pitr binlog \
  --host 127.0.0.1 \
  --port 3306 \
  --user root \
  --password-file /path/to/password \
  --server-id 9999 \
  --auto-resume \
  --compression zstd \
  --parallel 4 \
  --s3-bucket your-bucket \
  --s3-endpoint http://minio:9000 \
  --s3-path-style
```

程序持续运行，每 30 秒将已轮转的 binlog 文件上传到 S3，按 `Ctrl+C` 优雅退出。`--auto-resume` 启用断点续传，重启后自动从上次位置继续。

### 3. PITR 恢复

恢复到指定时间点：

```bash
mysql-pitr restore \
  --target-time 2026-04-13T10:30:00Z \
  --data-dir /var/lib/mysql \
  --target-host 127.0.0.1 \
  --target-port 3306 \
  --target-user root \
  --target-password-file /path/to/password \
  --mysql-shutdown-cmd "systemctl stop mysqld" \
  --mysql-start-cmd "systemctl start mysqld" \
  --parallel 4 \
  --s3-bucket your-bucket \
  --s3-endpoint http://minio:9000 \
  --s3-path-style
```

恢复到指定 GTID（更精确，推荐 GTID 启用的服务器）：

```bash
mysql-pitr restore \
  --target-gtid "3e11fa47-71ca-11e1-9e33:1-50" \
  --data-dir /var/lib/mysql \
  ...
```

### 4. 列出可用备份

```bash
# 表格格式
mysql-pitr list --s3-bucket your-bucket --s3-endpoint http://minio:9000 --s3-path-style

# JSON 格式
mysql-pitr list --json --s3-bucket your-bucket
```

### 5. 验证备份完整性

```bash
# 验证单个备份
mysql-pitr verify --backup-id full-20260413T100000Z --s3-bucket your-bucket

# 验证所有备份
mysql-pitr verify --all --s3-bucket your-bucket

# 完整验证（解压并 prepare，较慢）
mysql-pitr verify --backup-id full-20260413T100000Z --verify-data --s3-bucket your-bucket
```

### 6. 清理旧备份

```bash
# 预览（不实际删除）
mysql-pitr prune --retention-days 30 --dry-run --s3-bucket your-bucket

# 删除 30 天前的备份
mysql-pitr prune --retention-days 30 --s3-bucket your-bucket

# 保留最近 10 个备份
mysql-pitr prune --retention-count 10 --s3-bucket your-bucket

# 组合使用
mysql-pitr prune --retention-days 90 --retention-count 20 --s3-bucket your-bucket
```

## 命令详解

### backup - 全量备份

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--host` | MySQL 主机 | `127.0.0.1` |
| `--port` | MySQL 端口 | `3306` |
| `--user` | MySQL 用户 | `root` |
| `--password` | MySQL 密码（不安全） | |
| `--password-file` | 从文件读取密码（推荐） | |
| `--socket` | Unix socket（覆盖 host/port） | |
| `--xtrabackup-bin` | xtrabackup 路径 | `xtrabackup` |
| `--compression` | 压缩算法: none, gzip, zstd | `zstd` |
| `--parallel` | 并行线程数 | `4` |

### binlog - 持续拉取 binlog

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--host` | MySQL 主机 | `127.0.0.1` |
| `--port` | MySQL 端口 | `3306` |
| `--user` | MySQL 用户 | `root` |
| `--password` | MySQL 密码（不安全） | |
| `--password-file` | 从文件读取密码（推荐） | |
| `--server-id` | 复制服务器 ID（必须唯一） | `99999` |
| `--start-file` | 起始 binlog 文件 | |
| `--start-pos` | 起始位置 | `4` |
| `--auto-resume` | 自动从上次位置恢复 | `false` |
| `--compression` | 压缩算法: none, gzip, zstd | `none` |
| `--parallel` | 并行上传线程数 | `4` |

### restore - PITR 恢复

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--backup-id` | 指定备份 ID | 自动选择 |
| `--target-time` | 恢复目标时间 | |
| `--target-gtid` | 恢复目标 GTID | |
| `--data-dir` | MySQL 数据目录 | `/var/lib/mysql` |
| `--target-host` | 目标 MySQL 主机 | `127.0.0.1` |
| `--target-port` | 目标 MySQL 端口 | `3306` |
| `--target-user` | 目标 MySQL 用户 | `root` |
| `--target-password` | 目标 MySQL 密码 | |
| `--target-password-file` | 从文件读取密码（推荐） | |
| `--xtrabackup-bin` | xtrabackup 路径 | `xtrabackup` |
| `--mysqladmin-bin` | mysqladmin 路径 | `mysqladmin` |
| `--mysql-shutdown-cmd` | 停止 MySQL 命令 | |
| `--mysql-start-cmd` | 启动 MySQL 命令 | |
| `--mysql-start-timeout` | MySQL 启动超时 | `120s` |
| `--parallel` | 并行下载线程数 | `4` |
| `--copy-method` | 恢复方式: copy, move, hardlink | `copy` |

### verify - 验证备份

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--backup-id` | 备份 ID | |
| `--all` | 验证所有备份 | `false` |
| `--verify-data` | 解压并 prepare（慢） | `false` |

### prune - 清理旧备份

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--retention-days` | 保留天数 | `0`（禁用） |
| `--retention-count` | 保留数量 | `0`（禁用） |
| `--dry-run` | 预览不删除 | `false` |

## 全局 S3 参数

所有子命令均支持以下全局参数：

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--s3-bucket` | S3 存储桶名称 | 必填 |
| `--s3-endpoint` | 自定义 endpoint（MinIO 等） | 空（使用 AWS） |
| `--s3-region` | S3 区域 | `us-east-1` |
| `--s3-prefix` | S3 key 前缀 | `mysql-pitr/` |
| `--s3-path-style` | 使用 path-style 寻址（MinIO 必须开启） | `false` |
| `--s3-access-key` | Access Key ID | 环境变量 `AWS_ACCESS_KEY_ID` |
| `--s3-secret-key` | Secret Access Key | 环境变量 `AWS_SECRET_ACCESS_KEY` |
| `--s3-sse` | S3 服务端加密: AES256, aws:kms, aws:kms:dsse | |
| `--s3-sse-kms-key` | KMS 密钥 ID（SSE-KMS） | |
| `--webhook` | Webhook 通知 URL | |

S3 凭证优先级：命令行参数 > 环境变量 > AWS 默认凭证链。

## 安全特性

### 密码保护

推荐使用 `--password-file` 从文件读取密码，避免密码出现在命令行或脚本中：

```bash
# 创建密码文件
echo "your_password" > /etc/mysql-pitr/.password
chmod 600 /etc/mysql-pitr/.password

# 使用密码文件
mysql-pitr backup --password-file /etc/mysql-pitr/.password ...
```

密码优先级：`--password` > `--password-file` > `MYSQL_PWD` 环境变量

### S3 服务端加密

```bash
# 使用 AES256 加密
mysql-pitr backup --s3-sse AES256 ...

# 使用 KMS 加密
mysql-pitr backup --s3-sse aws:kms --s3-sse-kms-key arn:aws:kms:us-east-1:123456789:key/xxx ...
```

## S3 存储结构

```
<s3-prefix>/
├── full-20260413T100000Z/
│   ├── backup.xb.zst         # xtrabackup 流式备份（zstd 压缩）
│   └── backup.meta.json      # 备份元数据（binlog 位置、GTID、checksum 等）
├── full-20260414T020000Z/
│   ├── backup.xb.zst
│   └── backup.meta.json
└── binlogs/
    ├── binlog.000001.zst     # 压缩的 binlog
    ├── binlog.000002.zst
    ├── position.json         # 断点续传位置记录
    └── ...
```

## 恢复流程说明

```
全量备份(S3) ──→ 解压 ──→ xtrabackup --prepare
                                    │
                                    ▼
                           停止 MySQL 服务
                                    │
                                    ▼
                           xtrabackup --copy-back ──→ 启动 MySQL
                                                           │
                                                           ▼
                                              S3 binlogs ──→ mysqlbinlog ──→ mysql
                                              （流式管道，无需落盘）
```

## Webhook 通知

配置 Webhook 接收备份/恢复事件通知：

```bash
mysql-pitr backup --webhook https://your-server/webhook ...
```

事件类型：
- `backup_started` - 备份开始
- `backup_completed` - 备份完成
- `backup_failed` - 备份失败
- `restore_started` - 恢复开始
- `restore_completed` - 恢复完成
- `restore_failed` - 恢复失败

## 注意事项

- MySQL 用户需要有 `REPLICATION SLAVE` 和 `REPLICATION CLIENT` 权限才能拉取 binlog
- `--server-id` 必须在整个 MySQL 集群中唯一
- 恢复前确保 `--data-dir` 目录为空，或由 `--mysql-shutdown-cmd` 停止 MySQL 后清空
- 密码不会出现在日志中
- binlog 拉取进程崩溃后，使用 `--auto-resume` 可自动从上次位置继续
- 使用 `--copy-method move` 可加快恢复速度，但会清空备份目录
- 使用 `--copy-method hardlink` 需要备份和数据目录在同一文件系统

## License

MIT
