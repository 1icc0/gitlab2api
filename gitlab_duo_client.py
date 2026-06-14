import asyncio
import json
import re
import httpx
import websockets

GITLAB_HOST = "https://gitlab.com"
WSS_HOST = "wss://gitlab.com"

COOKIES = {
    "_gitlab_session": "cell-1-bf093c50d393e38b004c8b0cf754da17",
    "remember_user_token": "eyJfcmFpbHMiOnsibWVzc2FnZSI6Ilcxc3pPVEU1TlRrNE1sMHNJaVF5WVNReE15UjFWMk5pUm1WbVRHZnZMMlZ1TW5scGVHSm9NeTR1SWl3aU1UYzRNVFF5TVRrNE9TNHdOalF4TURFeUlsMD0iLCJleHAiOiIyMDI2LTA2LTI4VDA3OjI2OjI5LjA2NFoiLCJwdXIiOiJjb29raWUucmVtZW1iZXJfdXNlcl90b2tlbiJ9fQ%3D%3D--4836cf4d75a834a2339ce298f21a96e008cb5d78",
}

NAMESPACE_ID = "134747097"
MODEL = "claude_opus_4_8"
UA = "Mozilla/5.0 (Linux; Android 15; 2206122SC) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.7778.120 Mobile Safari/537.36"


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


def _resume_msg(workflow_id: str, goal: str, checkpoint: str) -> str:
    return json.dumps({
        "resumeRequest": {
            "workflowID": workflow_id,
            "clientVersion": "1.0",
            "goal": goal,
            "checkpoint": checkpoint,
            "approval": {},
        }
    })


def _parse_checkpoint(checkpoint_json: str) -> dict:
    try:
        return json.loads(checkpoint_json)
    except (json.JSONDecodeError, TypeError):
        return {}


def _extract_new_agent_content(checkpoint_json: str, seen_id: str | None) -> tuple[str | None, str | None]:
    """Return (content, message_id) for the latest agent entry whose id != seen_id."""
    inner = _parse_checkpoint(checkpoint_json)
    log = inner.get("channel_values", {}).get("ui_chat_log", [])
    for entry in reversed(log):
        if entry.get("message_type") == "agent":
            mid = entry.get("message_id")
            content = entry.get("content", "").strip()
            # mid != seen_id means this is a new reply from this turn
            if content and mid != seen_id:
                return content, mid
            # Same id = still the prior reply, nothing new yet
            break
    return None, None


async def _recv_until_done(ws, workflow_id: str, seen_id: str | None = None) -> tuple[str, str, str | None]:
    """Stream agent reply character-by-character, stop at INPUT_REQUIRED/COMPLETE.
    Returns (final_answer, last_checkpoint, new_agent_message_id).
    """
    last_checkpoint = ""
    final_answer = ""
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
            final_answer = content
            current_id = mid
            # Print only the newly arrived characters
            if len(content) > printed_len:
                print(content[printed_len:], end="", flush=True)
                printed_len = len(content)

        if status in ("INPUT_REQUIRED", "COMPLETE"):
            print()  # newline after streamed content
            if not final_answer:
                print(f"[debug] empty — status={status} checkpoint: {last_checkpoint[:500]}", flush=True)
            break
        elif status == "FAILED":
            print()
            raise RuntimeError(f"Workflow failed: {cp.get('errors', [])}")

    return final_answer, last_checkpoint, current_id


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
            print("[*] Fetching CSRF token...", flush=True)
            self._csrf = await fetch_csrf_token(self._http)
        if self.workflow_id is None:
            print("[*] Creating workflow...", flush=True)
            self.workflow_id = await create_workflow(self._http, self._csrf)
            print(f"[*] Workflow ID: {self.workflow_id}")

    async def send(self, message: str) -> str:
        await self._ensure_init()

        ws_headers = {
            "Cookie": cookie_header(),
            "Origin": "https://gitlab.com",
            "User-Agent": UA,
        }

        async with websockets.connect(_ws_url(self.workflow_id), additional_headers=ws_headers) as ws:
            # Always use startRequest — resumeRequest closes the connection immediately.
            # Pass the previous checkpoint to carry conversation history forward.
            payload = _start_msg(self.workflow_id, message, checkpoint=self.last_checkpoint)
            await ws.send(payload)

            answer, self.last_checkpoint, self.last_agent_id = await _recv_until_done(
                ws, workflow_id=self.workflow_id, seen_id=self.last_agent_id
            )

        return answer

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
                await session.send(user_input)
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
