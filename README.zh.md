# agui-acp-bridge

面向受支持的 [ACP v1](https://agentclientprotocol.com/) 子集的 Rust Bridge，提供 [AG-UI](https://docs.copilotkit.ai/ag-ui) HTTP/SSE 与 frontend-tool 网关。项目使用 ACP SDK 2.0.0 的稳定 v1 schema；并不承诺实现 ACP 的全部方法或能力。

- **ACP v1**：Bridge 与适配器实际使用的 JSON-RPC session/update 子集
- **AG-UI**：CopilotKit 等前端框架使用的 HTTP/SSE 事件协议

Bridge 把受支持的 ACP turn/update 翻译为 AG-UI 事件，并可选提供权限与 frontend-tool 通道。Agent 即使声明了子集之外的能力，Bridge 也不会因此实现这些能力。

English: [README.md](./README.md)

安全边界与当前依赖审计提示：[安全说明](./docs/SECURITY_NOTES.md)。

## 快速开始

启动一个进程内 Echo 网关（不需要外部 Agent 二进制）：

```bash
cargo run -p agui-acp-bridge-cli -- --in-process
```

另开一个终端发请求：

```bash
curl -N -X POST http://127.0.0.1:8080/ \
  -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  -d '{"threadId":"t1","runId":"r1","messages":[{"role":"user","id":"m1","content":"你好"}],"tools":[],"context":[],"forwardedProps":{},"state":{}}'
```

七个字段全部必填，camelCase；请发送 `Accept: text/event-stream`（缺省或 `*/*` 按 SSE 处理；显式的 protobuf `Accept` 会返回 `406`）。响应是 SSE 流，依次包含 `RUN_STARTED → TEXT_MESSAGE_START → TEXT_MESSAGE_CONTENT* → TEXT_MESSAGE_END → RUN_FINISHED`。

turn 进行期间，流每 15 秒发出一个名为 `agent:keepalive` 的 `CUSTOM` 事件，避免长时间空闲（长工具调用、等待权限决议）触发代理/负载均衡的 idle 超时。消费端应忽略其 value。其余 bridge `CUSTOM` 事件携带有效数据——`agent:session_init`、`agent:mode_update`、`agent:commands_available`、`agent:usage_update`，以及不透明透传的 `acp.session_update` / `acp.tool_call_raw_output`。

要换成真实 Agent 二进制，把 `--in-process` 换成可执行文件路径即可：

```bash
cargo run -p agui-acp-bridge-cli -- ./target/debug/examples/my_agent
```

## 工作原理

```
AG-UI 客户端 ──POST /──► BridgeHandler ──prompt()──► AcpSessionHandle
     ▲                       │                              │
     │                  Translator                   ACP 子进程 / 进程内
     └─────── SSE ───────────┘
```

Bridge 已实现显式映射：`thread_id` 是 AG-UI 的 bridge conversation key，并与一个
`AcpSessionHandle` 一一绑定；handle 中真实的 ACP `SessionId` 是独立身份。
同一 thread 的多轮对话复用同一个会话。Agent 通过 stdio 流式输出
`SessionUpdate`，由 `Translator` 翻译成 AG-UI 事件按序写到 SSE。流以
`RUN_FINISHED` 或 `RUN_ERROR` 终止。

## CLI

```text
agui-acp-bridge [OPTIONS] [-- AGENT_COMMAND...]

  --in-process                       使用内置 Echo Agent（与 AGENT_COMMAND 互斥）
  --host <IP>            127.0.0.1   绑定地址
  --port, -p <PORT>      8080        绑定端口
  --cwd, -w <DIR>        .           传给每个 ACP 会话的工作目录
  --policy <KIND>        auto-deny   auto-allow | auto-deny | allowlist | interrupt
  --allow <TITLE>                    allowlist 下批准的工具 title（可重复或逗号分隔）
  --permission-timeout <SEC>   300   推迟决策的权限请求兜底超时
  --idle-timeout <SEC>          120  空闲会话被回收阈值（in-flight prompt 不会被回收）
  --open-session-timeout <SEC> 30    ACP 握手超时
  --event-buffer <N>           64    per-prompt 事件 channel 容量
```

完整选项跑 `agui-acp-bridge --help`。日志走 stderr，用 `RUST_LOG` 调级别；Ctrl-C / SIGTERM 触发优雅退出。

### HTTP 端点

| 路径        | 方法 | 说明                                                          |
| ----------- | ---- | ------------------------------------------------------------- |
| `/`         | POST | AG-UI `RunAgentInput` → AG-UI 事件 SSE 流（16 MiB body 上限，公开 API 无法调整） |
| `/health`   | GET  | `200 {"status":"ok"}`                                          |
| `/sessions` | GET  | 通过 ACP `session/list` 列出 Agent 持久化的会话（不支持时返回 `501`） |
| `/approval` | POST | 决议被 `--policy interrupt` 推迟的权限请求                    |
| `/session/cancel` | POST | 取消缓存会话的当前 turn（不存在时返回 `404`） |
| `/session/close` | POST | Agent 声明 `sessionCapabilities.close` 时优雅关闭缓存 ACP 会话 |
| `/session/delete` | POST | Agent 声明 `sessionCapabilities.delete` 时从 `session/list` 移除持久化会话 |

`POST /` 因会话容量已满而无法接收 run 时返回 `503`；其他会话打开失败返回
`500`。这是 run 路由的响应，不是 `/approval` 的状态码。

`POST /session/close` 接收 `{ "threadId": "..." }`，并使用初始化阶段返回的
真实 ACP `SessionId`。成功返回 `204`；无缓存会话返回 `404`；存在 active/queued
turn、active setting 或 pending frontend/permission 工作时返回 `409`；Agent 未声明 close 时返回
`501`；ACP close error 返回 `502`；有界 close 超时返回 `504`。成功、失败和超时
都会移除本地会话及 pending frontend 状态；不支持 close 时保留会话以便继续复用。
空闲回收和 LRU 容量淘汰也会先复用同一 graceful-close 路径，再丢弃空闲 entry。
生命周期 close 或 eviction 占有 thread 期间，会话设置端点返回 `409`；普通设置
仍可排在 prompt 后执行。

`POST /session/delete` 接收 `{ "threadId": "..." }`，Agent 接受 ACP
`session/delete` 后返回 `204`。active 或 pending 本地工作返回 `409`；未声明 delete
返回 `501`；ACP error 返回 `502`；有界超时返回 `504`。Delete 的语义是从 Agent 的
`session/list` 中移除会话，ACP 不保证底层所有 artifact 都被硬删除。终态 delete
结果只清理精确的 bridge-owned `threadId` 映射和本地 frontend 状态；缓存未命中
（包括看起来像 ACP ID 的 thread ID）返回 `404` 且不发送 wire 请求。不支持
delete 时保留缓存会话以便复用。

ACP v1 content update 中的 `MessageId` 会映射到对应的 AG-UI 文本/推理
消息生命周期，并与 AG-UI `runId`、Bridge turn ID、MCP `toolCallId` 保持独立；
Bridge 生成的 synthetic event 使用自身的 fallback ID。

### 会话历史（`/sessions` + 恢复）

当 ACP Agent 声明了 `session/list` 与 `loadSession` 能力时，桥以**无状态**方式透传——自身不存任何历史：

- `GET /sessions` → `{"sessions":[{"sessionId","cwd","title?","updatedAt?"}]}`，其中
  `sessionId` 是 ACP 身份，不是 AG-UI `threadId`。每次查询会派生一个短生命周期
  Agent 连接，因此结果缓存 2 秒，并发请求合并为一次查询；错误不缓存。
- **恢复**会话：选择一个独立的 AG-UI `threadId`，并 POST 一个
  `forwardedProps` 含 `{"acpResume":{"sessionId":"<ACP sessionId>"}}` 的
  run。只有这个 typed marker 会启用私有恢复路径；布尔或格式错误的 marker 返回
  `ACP_RESUME_SESSION_ID_REQUIRED` 且不会打开 actor。缓存未命中时桥只用提供的
  ACP `sessionId` 发起 `session/load`，Agent 回放的历史会先以 AG-UI 事件流回前端，
  再进行新一轮对话。缓存命中时提供的 ACP ID 必须与该 thread 的映射一致，否则
  返回 `ACP_RESUME_FAILED`。若不支持 `loadSession` 或 `session/load` 失败，桥返回
  非成功 AG-UI resume error，绝不会回退为 `session/new`；不存在 ACP-ID alias 或
  fallback cleanup 路径。


`/approval` 请求体与状态码：

```json
{ "threadId": "<发起该 run 的 AG-UI threadId>", "interruptId": "<STATE_SNAPSHOT 中的 uuid>", "approved": true, "optionId": "allow_once" }
```

查找范围限定在 `threadId` 绑定的活跃会话——pending 权限只能在其所属 thread 上决议。

| 状态                       | 触发条件                                                       |
| -------------------------- | -------------------------------------------------------------- |
| `200 OK`                   | 决议送达 session actor                                         |
| `400 Bad Request`          | 请求体不是合法 JSON，或 `approved=true` 但缺 `optionId`          |
| `404 Not Found`            | 该 thread 无活跃会话，或 `interruptId` 不存在（已答复 / 超时 / 从未存在） |
| `422 Unprocessable Entity` | 缺 `threadId` 或其格式非法，或 `optionId` 不在 agent 候选集中（pending 请求保留以便重试） |

## 权限策略

ACP Agent 在执行工具前会发 `requestPermission`，Bridge 把决策委托给可插拔的 `PermissionPolicy`：

| 策略                    | 行为                                                                          |
| ----------------------- | ----------------------------------------------------------------------------- |
| `AutoDeny`（默认）      | 拒绝所有 `requestPermission`                                                  |
| `AutoAllow`             | 批准所有 `requestPermission`                                                  |
| `Allowlist`             | 仅批准 `title` 在配置集合中的工具调用                                         |
| `InterruptViaAgUiEvent` | 通过 `STATE_SNAPSHOT` 事件把决策交给前端，前端再走 `POST /approval` 答复     |

自定义策略：

```rust
use agent_client_protocol::schema::v1::RequestPermissionRequest;
use agui_acp_bridge_core::{PermissionDecision, PermissionPolicy};
use async_trait::async_trait;

#[derive(Debug)]
struct MyPolicy;

#[async_trait]
impl PermissionPolicy for MyPolicy {
    async fn decide(&self, req: &RequestPermissionRequest) -> PermissionDecision {
        // 检查 req.tool_call、req.options 等字段
        PermissionDecision::Deny
    }
}
```

## 作为库使用

最小集成（自己起监听器）：

```rust
use std::{path::PathBuf, sync::Arc};
use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, ProcessAcpClient, build_router,
};

let client: Arc<dyn AcpClient> = Arc::new(ProcessAcpClient::new("./my_agent"));
let state = BridgeAppState::new(client, PathBuf::from("."));
let router = build_router(state);
// axum::serve(listener, router).await?;
```

`ProcessAcpClient` 会通过 ACP 2.0 的结构化配置传递 `command`、每个
`with_args(...)` 参数和 `with_env(...)` 环境覆盖；含空格的 argv 会保持为
单个参数，环境变量也会真正传给子进程。`SessionConfig.cwd` 仍是 ACP 会话的
工作目录，不是进程启动配置。

`InProcessAcpClient::new()` 替换 `ProcessAcpClient` 即开发模式。需要换策略或调超时时用 builder：

```rust
BridgeAppState::builder(client, PathBuf::from("."))
    .with_policy(Arc::new(Allowlist::new(["Read file", "List directory"])))
    .with_config(BridgeConfig {
        idle_timeout: std::time::Duration::from_secs(600),
        ..BridgeConfig::default()
    })
    .build();
```

16 MiB body 上限覆盖所有路由（含 `POST /`），且无法通过公开 API 调整；超限的
AG-UI 请求体会返回 HTTP 413。

仓库结构：

| Crate                    | 职责                                                                                              |
| ------------------------ | ------------------------------------------------------------------------------------------------- |
| `agui-acp-bridge-core`   | `AcpClient` trait、`ProcessAcpClient` / `InProcessAcpClient`、`Translator`、`BridgeConfig`        |
| `agui-acp-bridge-policy` | `PermissionPolicy` 实现：`AutoAllow` / `AutoDeny` / `Allowlist` / `InterruptViaAgUiEvent`         |
| `agui-acp-bridge-server` | `BridgeHandler`、`BridgeAppState`、`build_router`、AG-UI SSE/会话路由、鉴权与 frontend-tool MCP |
| `agui-acp-bridge-cli`    | `agui-acp-bridge` 二进制                                                                          |

`crates/agui-acp-bridge-server/examples/` 下有三份端到端 demo：

```bash
cargo run -p agui-acp-bridge-server --example 02_in_process_dev      # 不需要 agent
cargo run -p agui-acp-bridge-server --example 01_subprocess_bridge -- ./my_agent
cargo run -p agui-acp-bridge-server --example 03_custom_policy     -- ./my_agent
```

## 测试与开发

Rust 检查（本地与 CI 使用同一组核心命令）：

```bash
cargo test --workspace --all-targets --locked
cargo test --workspace --all-targets --all-features --locked
cargo test --workspace --all-targets --no-default-features --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

Frontend 检查：

```bash
cd examples/copilotkit-acp-demo
bun install --frozen-lockfile
bun run type-check
bun run lint
bun run build
```

GitHub Actions 会在 Rust `1.88.0` 与 `stable` 上覆盖 default、all-features、
no-default-features 测试，并执行 stable fmt/Clippy、Bun 前端检查/构建和依赖审计。
前端 high advisories 只报告不阻断；critical advisories 会使 security job 失败。
可选的子进程 smoke 测试使用同级 `acp-rust` checkout，缺失时跳过；可用
`ACP_RUST_PATH` 指定其他位置。

## 依赖说明

网关依赖 crates.io 上已发布的 [`agui-rs`](https://crates.io/crates/agui-rs) 系列：`agui-rs-core`、`agui-rs-server`、`agui-rs-encoder`（均为 `0.1`）。

## 许可证

MIT OR Apache-2.0
