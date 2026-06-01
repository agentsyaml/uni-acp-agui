# agui-acp-bridge

把 [ACP](https://agentcommunicationprotocol.dev/) Agent 接入 [AG-UI](https://docs.copilotkit.ai/ag-ui) 前端的 Rust 网关。

- **ACP**：`acp-rust`、Claude Code、Kiro CLI 等 Agent 运行时使用的 JSON-RPC 协议
- **AG-UI**：CopilotKit 等前端框架使用的 HTTP/SSE 事件协议

任何符合 ACP 规范的 Agent，套上这一层就能直接对接 AG-UI 前端。当前 `cargo test --workspace` 77 通过、0 失败；CI 包含 `build / clippy / fmt / test`。

English: [README.md](./README.md)

## 快速开始

启动一个进程内 Echo 网关（不需要外部 Agent 二进制）：

```bash
cargo run -p agui-acp-bridge-cli -- --in-process
```

另开一个终端发请求：

```bash
curl -N -X POST http://localhost:8080/ \
  -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  -d '{"threadId":"t1","runId":"r1","messages":[{"role":"user","id":"m1","content":"你好"}],"tools":[],"context":[],"forwardedProps":{},"state":{}}'
```

七个字段全部必填，camelCase；`Accept: text/event-stream` 必带（暂未实现 protobuf 编码）。响应是 SSE 流，依次包含 `RUN_STARTED → TEXT_MESSAGE_START → TEXT_MESSAGE_CONTENT* → TEXT_MESSAGE_END → RUN_FINISHED`。

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

`thread_id` 与 `AcpSessionHandle` 一一绑定，同一 thread 的多轮对话复用同一个会话。Agent 通过 stdio 流式输出 `SessionUpdate`，由 `Translator` 翻译成 AG-UI 事件按序写到 SSE。流以 `RUN_FINISHED` 或 `RUN_ERROR` 终止。

## CLI

```text
agui-acp-bridge [OPTIONS] [-- AGENT_COMMAND...]

  --in-process                       使用内置 Echo Agent（与 AGENT_COMMAND 互斥）
  --host <IP>            0.0.0.0     绑定地址
  --port, -p <PORT>      8080        绑定端口
  --cwd, -w <DIR>        .           传给每个 ACP 会话的工作目录
  --policy <KIND>        auto-allow  auto-allow | auto-deny | allowlist | interrupt
  --allow <TITLE>                    allowlist 下批准的工具 title（可重复或逗号分隔）
  --permission-timeout <SEC>   300   推迟决策的权限请求兜底超时
  --idle-timeout <SEC>         1800  空闲会话被回收阈值（in-flight prompt 不会被回收）
  --open-session-timeout <SEC> 30    ACP 握手超时
  --event-buffer <N>           64    per-prompt 事件 channel 容量
```

完整选项跑 `agui-acp-bridge --help`。日志走 stderr，用 `RUST_LOG` 调级别；Ctrl-C / SIGTERM 触发优雅退出。

### HTTP 端点

| 路径        | 方法 | 说明                                                          |
| ----------- | ---- | ------------------------------------------------------------- |
| `/`         | POST | AG-UI `RunAgentInput` → AG-UI 事件 SSE 流（默认 16 MiB body 上限） |
| `/health`   | GET  | `200 {"status":"ok","sessions":N}`                            |
| `/sessions` | GET  | 通过 ACP `session/list` 列出 Agent 持久化的会话（不支持时返回 `501`） |
| `/approval` | POST | 决议被 `--policy interrupt` 推迟的权限请求                    |

### 会话历史（`/sessions` + 恢复）

当 ACP Agent 声明了 `session/list` 与 `loadSession` 能力时，桥以**无状态**方式透传——自身不存任何历史：

- `GET /sessions` → `{"sessions":[{"sessionId","cwd","title?","updatedAt?"}]}`，其中 `sessionId` 即用于恢复的 AG-UI `threadId`。
- **恢复**会话：POST 一个 `threadId` 等于该 `sessionId`、且 `forwardedProps` 含 `{"acpResume": true}` 的 run。缓存未命中时桥发起 `session/load`，Agent 回放的历史会先以 AG-UI 事件流回前端，再进行新一轮对话。若 Agent 不支持 `loadSession`，桥会透明回退为新建会话。


`/approval` 请求体与状态码：

```json
{ "interruptId": "<STATE_SNAPSHOT 中的 uuid>", "approved": true, "optionId": "allow_once" }
```

| 状态                       | 触发条件                                                       |
| -------------------------- | -------------------------------------------------------------- |
| `200 OK`                   | 决议送达 session actor                                         |
| `400 Bad Request`          | `approved=true` 但缺 `optionId`                                |
| `404 Not Found`            | `interruptId` 不存在（已答复 / 超时 / 从未存在）               |
| `422 Unprocessable Entity` | `optionId` 不在 agent 候选集中（pending 请求保留以便重试）     |

## 权限策略

ACP Agent 在执行工具前会发 `requestPermission`，Bridge 把决策委托给可插拔的 `PermissionPolicy`：

| 策略                    | 行为                                                                          |
| ----------------------- | ----------------------------------------------------------------------------- |
| `AutoAllow`（默认）     | 批准所有 `requestPermission`                                                  |
| `AutoDeny`              | 拒绝所有 `requestPermission`                                                  |
| `Allowlist`             | 仅批准 `title` 在配置集合中的工具调用                                         |
| `InterruptViaAgUiEvent` | 通过 `STATE_SNAPSHOT` 事件把决策交给前端，前端再走 `POST /approval` 答复     |

自定义策略：

```rust
use agent_client_protocol::schema::RequestPermissionRequest;
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

如果 16 MiB body 上限不合适，调用 `build_router_inner` 自己加 `DefaultBodyLimit` 层即可。

仓库结构：

| Crate                    | 职责                                                                                              |
| ------------------------ | ------------------------------------------------------------------------------------------------- |
| `agui-acp-bridge-core`   | `AcpClient` trait、`ProcessAcpClient` / `InProcessAcpClient`、`Translator`、`BridgeConfig`        |
| `agui-acp-bridge-policy` | `PermissionPolicy` 实现：`AutoAllow` / `AutoDeny` / `Allowlist` / `InterruptViaAgUiEvent`         |
| `agui-acp-bridge-server` | `BridgeHandler`、`BridgeAppState`、`build_router`（axum 路由 + `/health` + `/approval`）          |
| `agui-acp-bridge-cli`    | `agui-acp-bridge` 二进制                                                                          |

`crates/agui-acp-bridge-server/examples/` 下有三份端到端 demo：

```bash
cargo run -p agui-acp-bridge-server --example 02_in_process_dev      # 不需要 agent
cargo run -p agui-acp-bridge-server --example 01_subprocess_bridge -- ./my_agent
cargo run -p agui-acp-bridge-server --example 03_custom_policy     -- ./my_agent
```

## 测试与开发

```bash
just ci      # build + clippy + fmt + test 全跑通才算 OK
just test    # 仅跑测试
just fmt     # 格式化
```

子进程集成测试需要 `acp-rust` 同级 checkout，缺失时自动跳过；可通过 `ACP_RUST_PATH` 指定其他位置。设计决策与每轮重构记录见 [`REFACTOR_PROGRESS.md`](./REFACTOR_PROGRESS.md)。

## 依赖说明

网关依赖 crates.io 上已发布的 [`agui-rs`](https://crates.io/crates/agui-rs) 系列：`agui-rs-core`、`agui-rs-server`、`agui-rs-encoder`（均为 `0.1`）。

## 许可证

MIT OR Apache-2.0
