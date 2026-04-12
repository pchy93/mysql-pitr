# mysql-pitr

MySQL Point-In-Time Recovery (PITR) 工具，使用 Go 编写。基于 xtrabackup 进行全量备份，通过持续拉取 binlog 实现任意时间点恢复，备份文件存储于 S3 兼容对象存储。

## 功能特性

- **全量备份**：调用 `xtrabackup` 执行物理备份，打包后直接流式上传到 S3，无需双倍磁盘空间
- **持续 binlog 归档**：通过 `mysqlbinlog --read-from-remote-server` 实时拉取 binlog 并上传到 S3
- **时间点恢复**：从 S3 下载全量备份 + binlog，自动选择最近备份，恢复到指定时间点
- **支持 S3 兼容存储**：AWS S3、MinIO、Ceph 等均可使用

## 依赖

| 工具 | 用途 |
|------|------|
| `xtrabackup` | 全量备份与还原 |
| `mysqlbinlog` | binlog 拉取与应用 |
| `mysql` | 应用 binlog SQL |

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
  --password your_password \
  --s3-bucket your-bucket \
  --s3-endpoint http://minio:9000 \
  --s3-path-style \
  --s3-prefix mysql-pitr/
```

备份完成后输出 Backup ID，例如 `full-20260413T100000Z`。

### 2. 持续拉取 binlog（守护进程）

```bash
mysql-pitr binlog \
  --host 127.0.0.1 \
  --port 3306 \
  --user root \
  --password your_password \
  --server-id 9999 \
  --start-file binlog.000001 \
  --s3-bucket your-bucket \
  --s3-endpoint http://minio:9000 \
  --s3-path-style
```

程序持续运行，每 30 秒将已轮转的 binlog 文件上传到 S3，按 `Ctrl+C` 优雅退出（会上传当前文件后再退出）。

### 3. PITR 恢复

恢复到指定时间点：

```bash
mysql-pitr restore \
  --target-time 2026-04-13T10:30:00Z \
  --data-dir /var/lib/mysql \
  --target-host 127.0.0.1 \
  --target-port 3306 \
  --target-user root \
  --target-password your_password \
  --mysql-shutdown-cmd "systemctl stop mysqld" \
  --mysql-start-cmd "systemctl start mysqld" \
  --s3-bucket your-bucket \
  --s3-endpoint http://minio:9000 \
  --s3-path-style
```

不指定 `--target-time` 则恢复到最新可用位置。可通过 `--backup-id` 指定使用特定的全量备份。

### 4. 列出可用备份

```bash
# 表格格式
mysql-pitr list --s3-bucket your-bucket --s3-endpoint http://minio:9000 --s3-path-style

# JSON 格式
mysql-pitr list --json --s3-bucket your-bucket
```

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

S3 凭证优先级：命令行参数 > 环境变量 > AWS 默认凭证链。

## S3 存储结构

```
<s3-prefix>/
├── full-20260413T100000Z/
│   ├── backup.tar.gz       # xtrabackup 全量备份压缩包
│   └── backup.meta.json    # 备份元数据（binlog 位置、GTID、时间等）
├── full-20260414T020000Z/
│   ├── backup.tar.gz
│   └── backup.meta.json
└── binlogs/
    ├── binlog.000001
    ├── binlog.000002
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

## 注意事项

- MySQL 用户需要有 `REPLICATION SLAVE` 和 `REPLICATION CLIENT` 权限才能拉取 binlog
- `--server-id` 必须在整个 MySQL 集群中唯一
- 恢复前确保 `--data-dir` 目录为空，或由 `--mysql-shutdown-cmd` 停止 MySQL 后清空
- 密码不会出现在日志中
