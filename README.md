# Codex Proxy

Codex Proxy 是一个使用 Rust 编写的个人多账号代理，将 Codex 上游接口桥接为 OpenAI 兼容的 HTTP 接口。

## 架构与支持协议

- **OpenAI 兼容接口**：支持 `/v1/chat/completions`、`/v1/responses` 和 `/v1/models`，支持流式与非流式请求。
- **上游连接**：支持 WSS 和 HTTP SSE，默认使用 WSS。
- **账号管理**：支持 OAuth 登录、账号轮换、配额查询和失败重试。
- **状态监控**：提供 `/healthz` 健康检查和请求、配额日志。

代理会清理 Codex 不支持的请求参数，暂不支持 `previous_response_id`；流式响应开始后不会透明重试。

Chat 和 Responses 请求支持 `prompt_cache_key`，默认自动生成，也可通过 `server.auto_prompt_cache = false` 关闭自动生成。缓存命中取决于账号、上游和相同的长输入前缀。

## 构建与运行

### 依赖环境与编译

需要安装 Rust 和 Cargo。编译并运行测试：

```powershell
cargo test --locked
cargo build --release
```

Windows 编译产物位于 `target\release\codex-proxy.exe`。也可从 [Releases](https://github.com/preca-hoshino/codex-proxy/releases) 下载 Windows x64 或 ARM64 版本。

### 账号配置

```powershell
.\target\release\codex-proxy.exe account add --oauth
.\target\release\codex-proxy.exe account list
```

也可使用 `account import "$env:USERPROFILE\.codex\auth.json"` 导入已有登录。导入会复制刷新凭据，两个程序分别刷新可能导致原登录失效；独立授权更安全。

账号文件为明文 JSON，包含访问令牌和刷新令牌，请妥善保管。账号管理命令和手动编辑账号文件前，应先停止代理。

### 启动服务

```powershell
# 默认监听 127.0.0.1:8318
.\target\release\codex-proxy.exe serve

# 使用 HTTP SSE 连接上游
.\target\release\codex-proxy.exe serve --upstream-transport http
```

客户端 Base URL 为 `http://127.0.0.1:8318/v1`。非回环监听必须配置 `server.api_key`，客户端通过 Bearer Token 认证。

### 命令行工具

| 命令 | 用途 |
| --- | --- |
| `serve` | 启动代理服务 |
| `account` | 管理账号、刷新配额和重置冷却 |
| `models` | 查询上游模型目录 |
| `config` | 查看、检查或修改配置 |
| `test --model <MODEL> --stream` | 发送真实请求测试，消耗账号额度 |

运行程序或子命令并附加 `--help` 可查看选项。

## 配置

默认读取当前目录的 `config.toml`，文件不存在时使用内置默认值。配置项见 [config.example.toml](config.example.toml)，修改后需重启服务。排查请求清理或连接问题时，可设置 `RUST_LOG=info,codex_proxy=debug,proxy=debug` 启用调试日志。

## 致谢

模块划分和功能参考 [Antigravity Proxy](https://github.com/preca-hoshino/antigravity-proxy)；Codex 认证、上游协议与 Chat 转换参考 [zyycn/codex-proxy-rs](https://github.com/zyycn/codex-proxy-rs)（Apache-2.0）。

## 许可证

本项目基于 [Apache License 2.0](LICENSE) 分发。
