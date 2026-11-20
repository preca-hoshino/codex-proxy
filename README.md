# Codex Proxy

Codex Proxy 是一个使用 Rust 编写的个人多账号代理，将 Codex 上游接口桥接为 OpenAI 兼容的 HTTP 接口。

## 架构与支持协议

- **OpenAI 兼容接口**：支持 `/v1/chat/completions`、`/v1/responses` 和 `/v1/models`，支持流式与非流式请求。
- **上游连接**：支持 WSS 和 HTTP SSE，默认使用 WSS。
- **账号管理**：支持 OAuth 登录、账号轮换、配额查询和失败重试。
- **状态监控**：提供 `/healthz` 健康检查和请求、配额日志。

代理会自动清理不支持的请求参数，支持图片及多模态工具消息，暂不支持 `previous_response_id`；流式响应开始后不会透明重试。`prompt_cache_key` 默认自动生成（可通过 `server.auto_prompt_cache = false` 关闭），命中取决于账号、上游与相同长输入前缀。

## 构建与运行

需要安装 Rust 和 Cargo（Windows 也可从 [Releases](https://github.com/preca-hoshino/codex-proxy/releases) 下载 x64 或 ARM64 版本）：

```powershell
cargo test --locked
cargo build --release
```

Windows 编译产物位于 `target\release\codex-proxy.exe`。

### 账号配置与启动

```powershell
# 添加并查看账号（管理账号或编辑账号文件前应先停止代理）
.\target\release\codex-proxy.exe account add --oauth
.\target\release\codex-proxy.exe account list

# 启动服务（默认监听 127.0.0.1:8318，也可指定 --upstream-transport http）
.\target\release\codex-proxy.exe serve
```

- 也可通过 `account import "$env:USERPROFILE\.codex\auth.json"` 导入已有登录；共享刷新凭据可能导致原登录失效，独立授权更安全。
- 账号文件为明文 JSON（含访问与刷新令牌），请妥善保管；客户端 Base URL 为 `http://127.0.0.1:8318/v1`，非回环监听必须配置 `server.api_key`（Bearer Token 认证）。
- 账号列表和配额日志显示 5 小时及每周剩余额度和重置时间；单个配额窗口默认按每周显示，5 小时额度不适用时显示 `-`，上游明确标识 5 小时窗口时除外。`account list --json` 保留原有字段：`quota` / `quota_primary_reset` 对应 5 小时，`quota_secondary` / `quota_secondary_reset` 对应每周。

### 命令行工具

| 命令 | 用途 |
| --- | --- |
| `serve` | 启动代理服务 |
| `account` | 管理账号、刷新配额和重置冷却 |
| `models` | 查询上游模型目录 |
| `config` | 查看、检查或修改配置 |
| `test --model <MODEL> --stream` | 发送真实请求测试，消耗账号额度 |

附加 `--help` 可查看完整参数。

启动服务或运行 `models` 时，模型目录按模型信息合并并显示可用账号数量；不同账号的模型信息不同时分别显示。`models --json` 保留按账号分组的原始结果。

## 配置

默认读取当前目录的 `config.toml`（不存在时使用内置默认值），配置项见 [config.example.toml](config.example.toml)，修改后需重启服务。

日志级别可通过 `server.log_level`（默认 `info`）、全局参数 `--log-level` 或环境变量 `RUST_LOG` 控制；设置为 `debug` 时会输出上下游报文日志（`wire`），可通过 `server.log_redact_secrets = true`（默认 `false`）对凭据脱敏。

终端日志使用颜色和斜体强调标签；重定向输出或设置 `NO_COLOR` 时不添加终端样式。

## 致谢

模块划分和功能参考 [Antigravity Proxy](https://github.com/preca-hoshino/antigravity-proxy)；Codex 认证、上游协议与 Chat 转换参考 [zyycn/codex-proxy-rs](https://github.com/zyycn/codex-proxy-rs)（Apache-2.0）。

## 许可证

本项目基于 [Apache License 2.0](LICENSE) 分发。
