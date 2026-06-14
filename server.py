"""OpenAI-compatible API wrapper for GitLab Duo Chat.

Endpoints:
  GET  /v1/models
  POST /v1/chat/completions   (stream=true/false both supported)

Start: uvicorn server:app --host 0.0.0.0 --port 8000
  or:  python3 server.py
"""

import json
import time
import uuid
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from gitlab_duo_client import DuoChat, MODEL, _load_config

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_cfg = _load_config()
_srv = _cfg["server"]
API_KEYS: set[str] = set(_srv.get("api_keys", []))
SERVER_HOST: str = _srv.get("host", "0.0.0.0")
SERVER_PORT: int = int(_srv.get("port", 8000))

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="GitLab Duo OpenAI Proxy", docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# One session per process (single-user). Session is reset on error.
_session = DuoChat()


# ---------------------------------------------------------------------------
# Auth / error helpers
# ---------------------------------------------------------------------------

def _openai_error(status: int, code: str, message: str, param=None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={
            "error": {
                "message": message,
                "type": "invalid_request_error" if status < 500 else "server_error",
                "param": param,
                "code": code,
            }
        },
    )


def _check_auth(request: Request) -> JSONResponse | None:
    if not API_KEYS:
        return None
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return _openai_error(401, "invalid_api_key", "Missing or invalid Authorization header.")
    key = auth[len("Bearer "):]
    if key not in API_KEYS:
        return _openai_error(401, "invalid_api_key", "Incorrect API key provided.")
    return None


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class Message(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    model: str = MODEL
    messages: list[Message]
    stream: bool = False
    temperature: float | None = None
    max_tokens: int | None = None
    top_p: float | None = None
    presence_penalty: float | None = None
    frequency_penalty: float | None = None
    stop: list[str] | str | None = None
    user: str | None = None


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------

def _build_prompt(messages: list[Message]) -> str:
    """Merge system + user messages into a single prompt string.

    GitLab Duo doesn't have separate system/user roles, so we prepend
    any system messages as an instruction block before the user turn.
    """
    system_parts = [m.content for m in messages if m.role == "system"]
    # Find the last user message
    user_content = ""
    for m in reversed(messages):
        if m.role == "user":
            user_content = m.content
            break

    if not user_content:
        raise ValueError("No user message found in request.")

    if system_parts:
        system_block = "\n".join(system_parts)
        return f"[System]\n{system_block}\n\n[User]\n{user_content}"
    return user_content


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token for CJK/English mix."""
    return max(1, len(text) // 4)


# ---------------------------------------------------------------------------
# SSE helpers
# ---------------------------------------------------------------------------

def _sse(payload: dict) -> str:
    return "data: " + json.dumps(payload, ensure_ascii=False) + "\n\n"


def _chunk(req_id: str, content: str = "", finish_reason: str | None = None) -> str:
    delta = {"content": content} if content else {}
    return _sse({
        "id": req_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": MODEL,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    })


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/v1/models")
async def list_models(request: Request):
    if err := _check_auth(request):
        return err
    return {
        "object": "list",
        "data": [
            {
                "id": MODEL,
                "object": "model",
                "created": int(time.time()),
                "owned_by": "gitlab",
                "permission": [],
                "root": MODEL,
                "parent": None,
            }
        ],
    }


@app.post("/v1/chat/completions")
async def chat_completions(request: Request, body: ChatRequest):
    if err := _check_auth(request):
        return err

    try:
        prompt = _build_prompt(body.messages)
    except ValueError as e:
        return _openai_error(400, "invalid_request_error", str(e), param="messages")

    prompt_tokens = _estimate_tokens(prompt)
    req_id = f"chatcmpl-{uuid.uuid4().hex}"

    if body.stream:
        return StreamingResponse(
            _do_stream(prompt, req_id, prompt_tokens),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return await _do_complete(prompt, req_id, prompt_tokens)


async def _do_complete(prompt: str, req_id: str, prompt_tokens: int):
    global _session
    try:
        full = await _session.send(prompt)
    except Exception as e:
        _session.reset()
        return _openai_error(502, "upstream_error", str(e))

    completion_tokens = _estimate_tokens(full)
    return JSONResponse(
        content={
            "id": req_id,
            "object": "chat.completion",
            "created": int(time.time()),
            "model": MODEL,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": full},
                    "finish_reason": "stop",
                    "logprobs": None,
                }
            ],
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
            },
            "system_fingerprint": None,
        }
    )


async def _do_stream(prompt: str, req_id: str, prompt_tokens: int):
    global _session

    # Role delta
    yield _sse({
        "id": req_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": MODEL,
        "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": None}],
    })

    completion_tokens = 0
    try:
        async for chunk in _session.stream(prompt):
            completion_tokens += _estimate_tokens(chunk)
            yield _chunk(req_id, content=chunk)
    except Exception as e:
        _session.reset()
        yield _sse({"error": {"message": str(e), "type": "server_error", "code": "upstream_error"}})
        yield "data: [DONE]\n\n"
        return

    # Final chunk with finish_reason
    yield _sse({
        "id": req_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": MODEL,
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    })
    yield "data: [DONE]\n\n"


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host=SERVER_HOST, port=SERVER_PORT, reload=False)
