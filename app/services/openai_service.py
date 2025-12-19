# app/services/openai_service.py
import os, time, uuid, json, httpx
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI
from app.utils.retry import retry

load_dotenv()

# Initialize OpenAI client with custom timeout settings
timeout = httpx.Timeout(
    connect=20.0,   # TCP handshake / TLS
    read=180.0,     # *** <— extend this one ***
    write=60.0,     # file upload or request body
    pool=None       # keep-alive pool acquisition
)
transport = httpx.HTTPTransport(http2=False)
http_client = httpx.Client(transport=transport, timeout=timeout)

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),  # or just OPENAI_API if already set
    http_client=http_client,
    max_retries=3,                    # automatic retry for 5xx / timeouts
)

#______________________ new api calling percedure ________________________#

def _build_content(file_id: str | None, user_input: str | None):
    content = []
    if file_id:
        content.append({"type": "input_file", "file_id": file_id})
    content.append({"type": "input_text", "text": user_input or "Return JSON only."})
    return [{"role": "user", "content": content}]

def _extract_text_any(r: Any) -> str:
    # 1) 先用便捷欄位
    if getattr(r, "output_text", None):
        return r.output_text
    # 2) 保底：從 r.output[*].content[*].text 拼回
    out = getattr(r, "output", None)
    parts = []
    if isinstance(out, list):
        for item in out:
            content = getattr(item, "content", None)
            if isinstance(content, list):
                for c in content:
                    t = getattr(c, "text", None)
                    if isinstance(t, str):
                        parts.append(t)
                    elif isinstance(c, dict):
                        t2 = c.get("text")
                        if isinstance(t2, str):
                            parts.append(t2)
    return "".join(parts)

def _trim_to_json(text: str) -> str:
    text = (text or "").strip()
    j = text.rfind("}")
    return text[: j + 1] if j != -1 else text

@retry() 
def get_agent_response_bg(
    file_id: str | None = None,
    user_input: str | None = None,
    instructions: str | None = None,
    model: str = "gpt-5",
    poll_interval: float = 1.2,
    poll_timeout: int = 900,   # 最多等 15 分鐘（視任務調整）
) -> str:
    """背景執行 + 輪詢（不使用串流，繞過所有中途斷線問題）"""
    args = dict(
        model=model,
        instructions=instructions,
        input=_build_content(file_id, user_input),
        # 如果你的 SDK 版本支援，這行能讓 output_text 穩定有值；不支援會在下方 try/except 退回
        response_format={"type": "json_object"},
        extra_headers={"Idempotency-Key": str(uuid.uuid4())},
        background=True,
    )

    # 建立背景任務（兼容不支援 response_format 的舊版 SDK）
    try:
        job = client.responses.create(**args)
    except TypeError:
        args.pop("response_format", None)
        job = client.responses.create(**args)

    print("[bg] created:", job.id)
    t0 = time.time()
    last = None
    while True:
        r = client.responses.retrieve(job.id)
        st = getattr(r, "status", None)
        if st != last:
            print(f"[bg] status -> {st} (elapsed {round(time.time()-t0,1)}s)")
            last = st

        if st == "completed":
            text = _extract_text_any(r)
            return _trim_to_json(text)

        if st in ("failed", "cancelled"):
            print("[bg] failed detail:", getattr(r, "error", None))
            raise RuntimeError(f"Background {st}")

        if time.time() - t0 > poll_timeout:
            raise TimeoutError("Background polling timed out")

        time.sleep(poll_interval)