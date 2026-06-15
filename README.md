# GitLab Duo OpenAI Proxy

将 GitLab Duo Chat 包装成 OpenAI 兼容 API，让任何支持 OpenAI 接口的客户端（ChatBox、Open WebUI、Cursor、Continue、Python openai SDK 等）直接接入 GitLab Duo。

---

## 原理

GitLab Duo Chat 的底层通信协议是 WebSocket，流程如下：

```
客户端
  │
  ├─ POST /api/v4/ai/duo_workflows/workflows   创建 workflow，获得 workflow_id
  │
  └─ WSS  /api/v4/ai/duo_workflows/ws          建立 WebSocket 连接
       │
       ├─→ startRequest { goal, checkpoint, ... }   发送消息（含上轮 checkpoint 以保留历史）
       │
       └─← newCheckpoint { status, checkpoint }     流式接收回复
              │
              ├─ status=CREATED        内容还在生成中（checkpoint 里携带累积内容）
              ├─ status=INPUT_REQUIRED 本轮生成完毕，等待下一条用户消息
              └─ status=FAILED        出错
```

本项目在此协议之上加了一层 FastAPI 服务，对外暴露标准 OpenAI REST 接口，做了以下映射：

| OpenAI 概念 | GitLab Duo 实现 |
|---|---|
| 对话历史 | `checkpoint` JSON（GitLab 服务端维护） |
| system message | 拼接为 `[System]\n...\n[User]\n...` |
| stream=true | 逐 chunk 转发 SSE |
| stream=false | 等待 `INPUT_REQUIRED` 后一次性返回 |
| Bearer API Key | 本地校验，不透传给 GitLab |

---

## 文件说明

```
config.json            所有配置（cookies、api_keys、服务地址）
gitlab_duo_client.py   WebSocket 客户端核心逻辑
server.py              FastAPI 服务，暴露 OpenAI 兼容接口
```

---

## 快速开始

### 1. 安装依赖

```bash
pip install httpx websockets fastapi "uvicorn[standard]"
```

### 2. 获取 GitLab Cookie

用浏览器登录 [gitlab.com](https://gitlab.com)，打开开发者工具 → Application → Cookies，复制：

- `_gitlab_session`
- `remember_user_token`

### 3. 获取 namespace_id

可以通过 API 查询：

```bash
curl -s "https://gitlab.com/api/v4/namespaces?search=你的用户名" \
  -H "Cookie: _gitlab_session=..." | python3 -m json.tool
```
响应中的有两个id字段，第二个就是

### 4. 编辑 config.json

```json
{
  "gitlab": {
    "host": "https://gitlab.com",
    "namespace_id": "你的 namespace_id",
    "model": "claude_opus_4_8",
    "cookies": {
      "_gitlab_session": "粘贴 cookie 值",
      "remember_user_token": "粘贴 cookie 值"
    },
    "user_agent": "Mozilla/5.0 ..."
  },
  "server": {
    "host": "0.0.0.0",
    "port": 8000,
    "api_keys": [
      "sk-your-custom-key"
    ]
  }
}
```

`api_keys` 留空数组 `[]` 表示不鉴权（仅建议本地使用）。

### 5. 启动服务

```bash
python3 server.py
# 或
uvicorn server:app --host 0.0.0.0 --port 8000
```

启动后访问 `http://localhost:8000/v1/models` 验证服务正常。

---

## API 接口

### GET /v1/models

```bash
curl http://localhost:8000/v1/models \
  -H "Authorization: Bearer sk-your-custom-key"
```

### POST /v1/chat/completions

**非流式：**

```bash
curl http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer sk-your-custom-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude_opus_4_8",
    "messages": [
      {"role": "system", "content": "你是一个代码专家"},
      {"role": "user", "content": "用 Python 写一个快速排序"}
    ]
  }'
```

**流式：**

```bash
curl http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer sk-your-custom-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude_opus_4_8",
    "messages": [{"role": "user", "content": "你好"}],
    "stream": true
  }'
```

---

## 接入第三方客户端

所有客户端填写以下参数：

| 参数 | 值 |
|---|---|
| API Base URL | `http://localhost:8000/v1` |
| API Key | config.json 里设置的 key |
| 模型 | `claude_opus_4_8` |

### Python openai SDK

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8000/v1",
    api_key="sk-your-custom-key",
)

# 非流式
resp = client.chat.completions.create(
    model="claude_opus_4_8",
    messages=[{"role": "user", "content": "你好"}],
)
print(resp.choices[0].message.content)

# 流式
for chunk in client.chat.completions.create(
    model="claude_opus_4_8",
    messages=[{"role": "user", "content": "你好"}],
    stream=True,
):
    print(chunk.choices[0].delta.content or "", end="", flush=True)
```

### ChatBox / Open WebUI / Cursor

在设置中将 OpenAI API 地址改为 `http://localhost:8000/v1`，填入 API Key 即可，其余使用方式与官方 OpenAI 完全一致。

---

## 注意事项

- **Cookie 有效期**：`remember_user_token` 通常有效期约 2 周，过期后需重新获取。
- **单用户设计**：服务内部维护单个对话 session，多并发请求会共享同一上下文。如需多用户隔离，需自行扩展 session 管理。
- **对话历史**：历史由 GitLab 服务端通过 `checkpoint` 维护，重启服务后历史清空。
- **模型选择**：GitLab Duo 支持的模型取决于账户订阅等级，`claude_opus_4_8` 需要 GitLab Duo Pro 权限，但目前用户可试用30天。
