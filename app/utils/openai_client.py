import time, uuid, httpx
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI
from app.utils.retry import retry
from app.utils.common import _as_json_str,_json_safe
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
    http_client=http_client,
    max_retries=3,                    # automatic retry for 5xx / timeouts
)

#______________________ new api calling percedure ________________________#

def _build_content(file_id: str | None, user_input: str | None):
    user_text = _as_json_str(user_input) if user_input is not None else "Return JSON only."
    content = []
    if file_id:
        content.append({"type": "input_file", "file_id": file_id})
    content.append({"type": "input_text", "text": user_text})
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

def get_agent_response(
    file_id: str | None = None,
    user_input: str | None = None,
    instructions: str | None = None,
    model: str = "gpt-5.2",
    poll_interval: float = 1.2,
    poll_timeout: int = 3600,   # 最多等 60 分鐘（視任務調整）
    retry: int = 0,             # optional retry count
) -> str:
    """
    背景執行 + 輪詢（不使用串流，繞過所有中途斷線問題）
    Retries up to `retry` times if any error occurs.
    """
    last_error = None
    for attempt in range(retry + 1):  # include first try + retries
        try:
            args = dict(
                model=model,
                instructions=_as_json_str(instructions),
                input=_build_content(file_id, user_input),
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

            print(f"[bg] created: {job.id} (attempt {attempt+1}/{retry+1})")
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

        except Exception as e:
            last_error = e
            print(f"[error] attempt {attempt+1}/{retry+1} failed: {e}")
            if attempt < retry:
                time.sleep(2)  # backoff between retries
                continue
            else:
                raise last_error


def get_agent_response_quick(
        file_id: str | None = None,
        user_input: str | None = None,
        instructions: str | None = None,
):
        """
        呼叫 OpenAI。

        Parameters
        ----------
        file_id : str | None               # 若提供則附加 input_file
        user_input : str | None            # 自訂 user prompt
        instructions : str | None          # model system instructions
        """
        # 1. 建構 input_content
        input_content: list[dict] = []

        if file_id:                        # ← 只有 file_id 有值才加入
                input_content.append({"type": "input_file", "file_id": file_id})

        prompt = (
                user_input
                if user_input is not None
                else "Provide response in JSON format, do not add any additional text!"
        )
        input_content.append({"type": "input_text", "text": prompt})

        stream = client.responses.create(
                model="gpt-5-mini",
                instructions=instructions,
                input=[
                    {"role": "user", "content": input_content},
                    
                ],
                #temperature=0,
                tools=[{"type":"web_search"}],
                tool_choice="auto",
                stream=True,  # 啟用流式輸出
        )
        
        full_text = ""
        for event in stream:
                if event.type == "response.output_text.delta":
                        full_text += event.delta          # event.delta 是新 token
                        #print(event.delta, end="", flush=True)
                elif event.type == "response.output_text.done":
                        print()                           # optional：段落完換行
                elif event.type == "response.done":
                        break
        
        return full_text
        #print("\n---\n完整文字：", full_text)


def get_agent_response_tools(
    file_id: str | None = None,
    user_input: str | None = None,
    instructions: str | None = None,
    model: str = "gpt-5.2",
    poll_interval: float = 1.2,
    poll_timeout: int = 900,
    tools: list | None = None,
    include: list | None = None,
) -> str:
    """背景執行 + 輪詢（不使用串流，繞過所有中途斷線問題），不再限制 max_output_tokens"""

    args = dict(
        model=model,
        instructions=instructions,
        input=_build_content(file_id, user_input),
        extra_headers={"Idempotency-Key": str(uuid.uuid4())},
        background=True,
        # optional：叫 gpt-5 少啲 reasoning，省 token（可保留 / 移除）
        # "reasoning": {"effort": "low"},
    )

    if tools:
        args["tools"] = tools
    if include:
        args["include"] = include

    job = client.responses.create(**args)

    print("[bg] created:", job.id)
    t0 = time.time()
    last = None

    while True:
        r = client.responses.retrieve(job.id)
        st = getattr(r, "status", None)

        if st != last:
            print(f"[bg] status -> {st} (elapsed {round(time.time() - t0, 1)}s)")
            last = st

        if st in ("completed", "incomplete"):
            if st == "incomplete":
                print("[bg] warning: response is INCOMPLETE")
                try:
                    print("incomplete_details:", getattr(r, "incomplete_details", None))
                except Exception:
                    pass

            text = _extract_text_any(r)
            return _trim_to_json(text)

        if st in ("failed", "cancelled"):
            print("[bg] failed detail:", getattr(r, "error", None))
            raise RuntimeError(f"Background {st}")

        if time.time() - t0 > poll_timeout:
            raise TimeoutError("Background polling timed out")

        time.sleep(poll_interval)