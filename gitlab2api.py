import asyncio
import json
import re
import httpx
import websockets
from pathlib import Path
from typing import AsyncIterator


def _load_config() -> dict:
    path = Path(__file__).parent / "config.json"
    with open(path) as f:
        return json.load(f)


_cfg = _load_config()
_gl = _cfg["gitlab"]

GITLAB_HOST: str = _gl["host"]
WSS_HOST: str = GITLAB_HOST.replace("https://", "wss://").replace("http://", "ws://")
NAMESPACE_ID: str = str(_gl["namespace_id"])
MODEL: str = _gl["model"]
UA: str = _gl["user_agent"]
COOKIES: dict = _gl["cookies"]


def cookie_header() -> str:
    return "; ".join(f"{k}={v}" for k, v in COOKIES.items())


async def fetch_csrf_token(client: httpx.AsyncClient) -> str:
    for path in ["/dashboard", "/users/sign_in"]:
        resp = await client.get(
            f"{GITLAB_HOST}{path}",
            headers={"User-Agent": UA, "Cookie": cookie_header()},
            follow_redirects=True,
        )
        m = re.search(r'<meta\s+name=["\']csrf-token["\']\s+content=["\'](.*?)["\']', resp.text)
        if m:
            return m.group(1)
    raise RuntimeError("Could not find CSRF token — are the cookies still valid?")


async def create_workflow(client: httpx.AsyncClient, csrf: str) -> str:
    resp = await client.post(
        f"{GITLAB_HOST}/api/v4/ai/duo_workflows/workflows",
        json={"namespace_id": NAMESPACE_ID, "workflow_definition": "chat"},
        headers={
            "Content-Type": "application/json",
            "Cookie": cookie_header(),
            "User-Agent": UA,
            "X-Requested-With": "XMLHttpRequest",
            "X-CSRF-Token": csrf,
        },
    )
    if not resp.is_success:
        raise RuntimeError(f"Failed to create workflow ({resp.status_code}): {resp.text[:200]}")
    return str(resp.json()["id"])


def _ws_url(workflow_id: str) -> str:
    return (
        f"{WSS_HOST}/api/v4/ai/duo_workflows/ws"
        f"?root_namespace_id={NAMESPACE_ID}&namespace_id={NAMESPACE_ID}"
        f"&user_selected_model_identifier={MODEL}"
        f"&workflow_definition=chat&workflow_id={workflow_id}&client_type=browser"
    )


def _start_msg(workflow_id: str, goal: str, checkpoint: str = "") -> str:
    msg: dict = {
        "startRequest": {
            "workflowID": workflow_id,
            "clientVersion": "1.0",
            "workflowDefinition": "chat",
            "workflowMetadata": json.dumps({
                "extended_logging": False,
                "is_team_member": False,
                "tool_approval_for_session_enabled": True,
            }),
            "clientCapabilities": ["incremental_streaming", "web_search"],
            "goal": goal,
            "approval": {},
            "useOrbit": False,
            "additional_context": [],
        }
    }
    if checkpoint:
        msg["startRequest"]["checkpoint"] = checkpoint
    return json.dumps(msg)


def _parse_checkpoint(checkpoint_json: str) -> dict:
    try:
        return json.loads(checkpoint_json)
    except (json.JSONDecodeError, TypeError):
        return {}


def _extract_new_agent_content(checkpoint_json: str, seen_id: str | None) -> tuple[str | None, str | None]:
    inner = _parse_checkpoint(checkpoint_json)
    log = inner.get("channel_values", {}).get("ui_chat_log", [])
    for entry in reversed(log):
        if entry.get("message_type") == "agent":
            mid = entry.get("message_id")
            content = entry.get("content", "").strip()
            if content and mid != seen_id:
                return content, mid
            break
    return None, None


async def _stream_ws(ws, seen_id: str | None = None) -> AsyncIterator[tuple[str, str, str | None]]:
    last_checkpoint = ""
    current_id = seen_id
    printed_len = 0

    async for raw in ws:
        data = json.loads(raw)
        if "newCheckpoint" not in data:
            if "error" in data:
                raise RuntimeError(f"Server error: {data['error']}")
            continue

        cp = data["newCheckpoint"]
        status = cp.get("status", "")
        last_checkpoint = cp.get("checkpoint", last_checkpoint)

        content, mid = _extract_new_agent_content(last_checkpoint, seen_id)
        if content:
            current_id = mid
            if len(content) > printed_len:
                chunk = content[printed_len:]
                printed_len = len(content)
                yield chunk, last_checkpoint, current_id

        if status in ("INPUT_REQUIRED", "COMPLETE"):
            break
        elif status == "FAILED":
            raise RuntimeError(f"Workflow failed: {cp.get('errors', [])}")

    yield "", last_checkpoint, current_id


class DuoChat:
    def __init__(self):
        self.workflow_id: str | None = None
        self.last_checkpoint: str = ""
        self.last_agent_id: str | None = None
        self._csrf: str | None = None
        self._http: httpx.AsyncClient | None = None

    async def _ensure_init(self):
        if self._http is None:
            self._http = httpx.AsyncClient(follow_redirects=True)
        if self._csrf is None:
            self._csrf = await fetch_csrf_token(self._http)
        if self.workflow_id is None:
            self.workflow_id = await create_workflow(self._http, self._csrf)

    async def send(self, message: str) -> str:
        parts = []
        async for chunk in self.stream(message):
            parts.append(chunk)
        return "".join(parts)

    async def stream(self, message: str) -> AsyncIterator[str]:
        await self._ensure_init()
        ws_headers = {
            "Cookie": cookie_header(),
            "Origin": GITLAB_HOST,
            "User-Agent": UA,
        }
        async with websockets.connect(_ws_url(self.workflow_id), additional_headers=ws_headers) as ws:
            payload = _start_msg(self.workflow_id, message, checkpoint=self.last_checkpoint)
            await ws.send(payload)
            async for chunk, checkpoint, mid in _stream_ws(ws, seen_id=self.last_agent_id):
                self.last_checkpoint = checkpoint
                self.last_agent_id = mid
                if chunk:
                    yield chunk

    def reset(self):
        self.workflow_id = None
        self.last_checkpoint = ""
        self.last_agent_id = None
        self._csrf = None

    async def close(self):
        if self._http:
            try:
                await self._http.aclose()
            except Exception:
                pass
            self._http = None


async def repl():
    print("GitLab Duo Chat  (type 'exit' or Ctrl-C to quit)\n")
    session = DuoChat()
    try:
        while True:
            try:
                user_input = input("You: ").strip()
            except EOFError:
                break
            if not user_input:
                continue
            if user_input.lower() in ("exit", "quit", "/exit", "/quit"):
                break
            print("Assistant: ", end="", flush=True)
            try:
                async for chunk in session.stream(user_input):
                    print(chunk, end="", flush=True)
                print()
            except RuntimeError as e:
                print(f"\n[Error] {e}")
    except KeyboardInterrupt:
        print("\nBye.")
    finally:
        await session.close()


if __name__ == "__main__":
    try:
        asyncio.run(repl())
    except KeyboardInterrupt:
        pass
