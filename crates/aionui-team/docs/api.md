# Team HTTP API

所有端点前缀 `/api/teams`，全部需要 JWT。响应统一 `ApiResponse<T>` / `ErrorResponse`。DTO 定义在 `crates/aionui-api-types/src/team.rs` 与 `team_work_command.rs`。

## 端点一览

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/teams` | 创建团队（请求必须显式包含且仅包含一个 lead） |
| GET | `/api/teams` | 列出当前用户拥有的团队 |
| GET | `/api/teams/{id}` | 获取单个团队详情 |
| DELETE | `/api/teams/{id}` | 删除团队（级联删 agents / mailbox / WorkItems） |
| PATCH | `/api/teams/{id}/name` | 重命名团队 |
| POST | `/api/teams/{id}/agents` | 新增 agent |
| DELETE | `/api/teams/{id}/agents/{slot_id}` | 移除 agent |
| PATCH | `/api/teams/{id}/agents/{slot_id}/name` | 重命名 agent |
| POST | `/api/teams/{id}/session` | 启动/确保 session 在跑（幂等） |
| DELETE | `/api/teams/{id}/session` | 停止 session |

消息端点仍由 Team 路由提供，实时会话/历史投影与普通单聊共用 conversation adapter：

- `POST /api/teams/{id}/messages` — 发送给 Team lead
- `POST /api/teams/{id}/agents/{slot_id}/messages` — 发送给指定成员

**WorkItem 端点**：
- `GET /api/teams/{id}/work-items` — 当前用户可见的 WorkItem 投影
- `GET /api/teams/{id}/work-items/{work_item_id}` — WorkItem、Delivery 和状态快照
- `GET /api/teams/{id}/work-items/{work_item_id}/events` — append-only 事件历史
- `POST /api/teams/{id}/work-items/delegate` — owner 委派 WorkItem
- `POST /api/teams/{id}/work-items/{work_item_id}/review` — Lead 审核提交
- `POST /api/teams/{id}/work-items/{work_item_id}/cancel` — Lead 取消未完成 WorkItem
- `POST /api/teams/{id}/work-items/{work_item_id}/integrate` — 对已接受 Git Delivery 执行显式集成或恢复

`request_changes` 必须提供 trim 后 1..4000 字符的 `feedback`；`accept` 和 `reject` 不接受非空反馈。WorkItem 状态、事件与派生 mailbox 通知在同一 SQLite 事务中提交；启动/周期恢复器只负责恢复丢失的内存 wake。

Mailbox 不是 WorkItem 的事实来源；普通跨成员消息仍走上面的 Team message 路由，内部 mailbox 只负责运行时投递、唤醒与回执。

## 关键字段

### CreateTeamRequest
```json
{
  "name": "string",
  "workspace": "/absolute/project/root",
  "agents": [
    {
      "name": "Lead",
      "role": "lead",
      "model": "default",
      "assistant_id": "bare:8e1acf31"
    },
    {
      "name": "Worker",
      "role": "teammate",
      "model": "default",
      "assistant_id": "bare:8e1acf31"
    }
  ]
}
```

- `agents` 至少 1 个，且必须恰好一个 `role: "lead"`（兼容输入 `leader`）。数组顺序不授予权限。
- 每个成员必须引用可用于 Team 的 `assistant_id`；backend、skills、MCP 和运行时配置由 Assistant 快照解析，不接受客户端重复声明。
- `conversation_id` 必须省略。Core 为每个成员创建独立 conversation，并把 Team 身份绑定为不可变字段。
- `workspace` 可省略；提供时必须是合法绝对目录。所有成员共享逻辑项目根，Git WorkItem 再由服务端派生隔离 worktree。

### TeamResponse
```json
{
  "id": "t_xxx",
  "user_id": "u_xxx",
  "name": "string",
  "workspace": "/absolute/project/root",
  "workspace_mode": "shared",
  "assistants": [ TeamAgentResponse ],
  "leader_assistant_id": "slot_xxx",
  "session_mode": "auto",
  "created_at": 1730000000000,
  "updated_at": 1730000000000
}
```

### TeamAgentResponse
```json
{
  "slot_id": "slot_xxx",
  "assistant_name": "Worker",
  "name": "string",
  "role": "lead | teammate",
  "conversation_id": "conv_xxx",
  "assistant_backend": "codex",
  "backend": "string",
  "model": "string",
  "assistant_id": "bare:8e1acf31",
  "status": "idle | active | completed | failed",
  "pending_confirmations": 0
}
```
`slot_id` 是 Team 权限、mailbox 和 WorkItem 的成员身份；`conversation_id` 只用于打开该成员的持久会话历史，二者不能互换或由客户端伪造。

## 消息与历史

用户输入必须走 Team 路由，确保 TeamRun、目标 slot、幂等回执和 mailbox 投递保持一致：

| 动作 | 端点 |
|------|------|
| 用户给 Lead 发消息 | `POST /api/teams/{id}/messages` |
| 用户给指定成员发消息 | `POST /api/teams/{id}/agents/{slot_id}/messages` |
| 拉成员会话历史 | `GET /api/conversations/{conversation_id}/messages` |

发送成功返回 `TeamRunAckResponse` envelope：`{ enqueue_status, message_id, run }`。客户端必须校验 `run.team_id` 以及目标 slot；不要把 envelope 当成扁平 TeamRun。

Team session 通常由会话首发或 durable mailbox 恢复器按需启动；显式 `POST /api/teams/{id}/session` 仍是幂等的预热接口。Agent 之间使用原生 `team_send_message`，WorkItem 的委派、提交、反馈和取消通知由 canonical command 在事务内生成，不依赖飞书评论或前端二次发送。

## 错误码

| 错误 | HTTP |
|------|------|
| `TeamNotFound` / `AgentNotFound` | 404 |
| `SessionNotFound` | 404 |
| `InvalidRequest` | 400 |
