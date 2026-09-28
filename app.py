import argparse
import asyncio
import dataclasses
import json
import logging
import os
import random
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger("async-ai-task-engine")

STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_ERROR = "ERROR"
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

SCHEMA = """
CREATE TABLE IF NOT EXISTS task_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    task_id INTEGER NOT NULL,
    prompt TEXT NOT NULL,
    response TEXT,
    status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED', 'ERROR')),
    error TEXT,
    http_status INTEGER,
    attempts INTEGER NOT NULL,
    latency_ms REAL NOT NULL,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    total_tokens INTEGER,
    model TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_task_logs_run_id ON task_logs (run_id);
CREATE INDEX IF NOT EXISTS idx_task_logs_status ON task_logs (status);
"""

INSERT_SQL = """
INSERT INTO task_logs (
    run_id, task_id, prompt, response, status, error, http_status, attempts,
    latency_ms, prompt_tokens, completion_tokens, total_tokens, model, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


@dataclass(frozen=True)
class Settings:
    api_key: str
    base_url: str
    model: str
    concurrency: int
    timeout: float
    max_retries: int
    db_path: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            api_key=os.getenv("OPENAI_API_KEY", "").strip(),
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/"),
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            concurrency=int(os.getenv("MAX_CONCURRENCY", "5")),
            timeout=float(os.getenv("REQUEST_TIMEOUT", "30")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            db_path=os.getenv("DB_PATH", "tasks.db"),
        )


@dataclass
class TaskResult:
    task_id: int
    prompt: str
    status: str
    response: Optional[str]
    error: Optional[str]
    http_status: Optional[int]
    attempts: int
    latency_ms: float
    prompt_tokens: Optional[int]
    completion_tokens: Optional[int]
    total_tokens: Optional[int]
    model: str
    created_at: str


class Database:
    def __init__(self, path: str) -> None:
        parent = Path(path).resolve().parent
        parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def _insert(self, run_id: str, result: TaskResult) -> None:
        with self._lock:
            self._conn.execute(
                INSERT_SQL,
                (
                    run_id,
                    result.task_id,
                    result.prompt,
                    result.response,
                    result.status,
                    result.error,
                    result.http_status,
                    result.attempts,
                    result.latency_ms,
                    result.prompt_tokens,
                    result.completion_tokens,
                    result.total_tokens,
                    result.model,
                    result.created_at,
                ),
            )
            self._conn.commit()

    async def save(self, run_id: str, result: TaskResult) -> None:
        await asyncio.to_thread(self._insert, run_id, result)

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def parse_completion(body: str) -> tuple[str, dict]:
    data = json.loads(body)
    if not isinstance(data, dict):
        raise ValueError("response is not a JSON object")
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("missing 'choices' in response")
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        raise ValueError("empty completion content")
    usage = data.get("usage")
    return content.strip(), usage if isinstance(usage, dict) else {}


async def execute_task(
    session: aiohttp.ClientSession,
    settings: Settings,
    task_id: int,
    prompt: str,
) -> TaskResult:
    url = f"{settings.base_url}/chat/completions"
    payload = {
        "model": settings.model,
        "messages": [{"role": "user", "content": prompt}],
    }
    started = time.perf_counter()
    status = STATUS_ERROR
    error: Optional[str] = None
    http_status: Optional[int] = None
    content: Optional[str] = None
    usage: dict = {}
    attempts = 0

    for attempt in range(1, settings.max_retries + 2):
        attempts = attempt
        retryable = False
        try:
            async with session.post(url, json=payload) as resp:
                http_status = resp.status
                body = await resp.text()
            if http_status == 200:
                try:
                    content, usage = parse_completion(body)
                    status = STATUS_SUCCESS
                    error = None
                except ValueError as exc:
                    status = STATUS_FAILED
                    error = f"Response validation failed: {exc}"
                break
            status = STATUS_FAILED
            error = f"HTTP {http_status}: {body[:300]}"
            retryable = http_status in RETRYABLE_STATUSES
        except asyncio.TimeoutError:
            status = STATUS_ERROR
            error = f"Request timed out after {settings.timeout}s"
            retryable = True
        except aiohttp.ClientError as exc:
            status = STATUS_ERROR
            error = f"{type(exc).__name__}: {exc}"
            retryable = True

        if not retryable or attempt > settings.max_retries:
            break
        delay = min(2 ** (attempt - 1), 30) + random.random()
        logger.warning(
            "task=%d attempt=%d failed (%s); retrying in %.1fs",
            task_id,
            attempt,
            error,
            delay,
        )
        await asyncio.sleep(delay)

    return TaskResult(
        task_id=task_id,
        prompt=prompt,
        status=status,
        response=content,
        error=error,
        http_status=http_status,
        attempts=attempts,
        latency_ms=round((time.perf_counter() - started) * 1000, 2),
        prompt_tokens=usage.get("prompt_tokens"),
        completion_tokens=usage.get("completion_tokens"),
        total_tokens=usage.get("total_tokens"),
        model=settings.model,
        created_at=datetime.now(timezone.utc).isoformat(),
    )


async def worker(
    name: str,
    queue: asyncio.Queue,
    session: aiohttp.ClientSession,
    settings: Settings,
    db: Database,
    run_id: str,
    results: list,
) -> None:
    while True:
        task_id, prompt = await queue.get()
        try:
            result = await execute_task(session, settings, task_id, prompt)
            await db.save(run_id, result)
            results.append(result)
            logger.info(
                "%s task=%d status=%s http=%s attempts=%d latency=%.0fms",
                name,
                result.task_id,
                result.status,
                result.http_status,
                result.attempts,
                result.latency_ms,
            )
        except Exception:
            logger.exception("%s task=%d crashed unexpectedly", name, task_id)
        finally:
            queue.task_done()


async def run_engine(prompts: list[str], settings: Settings, db: Database, run_id: str):
    queue: asyncio.Queue = asyncio.Queue()
    for task_id, prompt in enumerate(prompts, start=1):
        queue.put_nowait((task_id, prompt))

    results: list[TaskResult] = []
    workers_count = max(1, min(settings.concurrency, len(prompts)))
    timeout = aiohttp.ClientTimeout(total=settings.timeout)
    connector = aiohttp.TCPConnector(limit=workers_count)
    headers = {
        "Authorization": f"Bearer {settings.api_key}",
        "Content-Type": "application/json",
    }

    started = time.perf_counter()
    async with aiohttp.ClientSession(
        timeout=timeout, connector=connector, headers=headers
    ) as session:
        tasks = [
            asyncio.create_task(
                worker(f"worker-{i}", queue, session, settings, db, run_id, results)
            )
            for i in range(1, workers_count + 1)
        ]
        await queue.join()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return results, time.perf_counter() - started


def load_prompts(input_path: Optional[str], inline: list[str]) -> list[str]:
    prompts = [p.strip() for p in inline if p and p.strip()]
    if input_path:
        path = Path(input_path)
        if not path.is_file():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() == ".json":
            data = json.loads(text)
            if not isinstance(data, list):
                raise ValueError("JSON input must be a list")
            for item in data:
                value = item.get("prompt") if isinstance(item, dict) else item
                if isinstance(value, str) and value.strip():
                    prompts.append(value.strip())
        else:
            prompts.extend(line.strip() for line in text.splitlines() if line.strip())
    return prompts


def print_summary(results: list[TaskResult], wall_time: float, run_id: str) -> None:
    counts = {STATUS_SUCCESS: 0, STATUS_FAILED: 0, STATUS_ERROR: 0}
    for result in results:
        counts[result.status] += 1
    sequential_estimate = sum(r.latency_ms for r in results) / 1000
    speedup = sequential_estimate / wall_time if wall_time > 0 else 0.0
    tokens = sum(r.total_tokens or 0 for r in results)
    avg_latency = (
        sum(r.latency_ms for r in results) / len(results) if results else 0.0
    )
    print()
    print(f"Run ID              : {run_id}")
    print(f"Total tasks         : {len(results)}")
    print(f"SUCCESS             : {counts[STATUS_SUCCESS]}")
    print(f"FAILED              : {counts[STATUS_FAILED]}")
    print(f"ERROR               : {counts[STATUS_ERROR]}")
    print(f"Avg latency         : {avg_latency:.0f} ms")
    print(f"Total tokens        : {tokens}")
    print(f"Wall time           : {wall_time:.2f} s")
    print(f"Sequential estimate : {sequential_estimate:.2f} s")
    print(f"Speedup             : {speedup:.2f}x")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Async AI Task & Processing Engine"
    )
    parser.add_argument("-i", "--input", help="Path to .txt (one prompt per line) or .json (list)")
    parser.add_argument("-p", "--prompt", action="append", default=[], help="Inline prompt (repeatable)")
    parser.add_argument("-c", "--concurrency", type=int, help="Number of parallel workers")
    parser.add_argument("-m", "--model", help="Model name")
    parser.add_argument("--timeout", type=float, help="Request timeout in seconds")
    parser.add_argument("--retries", type=int, help="Max retries per task")
    parser.add_argument("--db", help="SQLite database path")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = Settings.from_env()
    overrides = {
        "concurrency": args.concurrency,
        "model": args.model,
        "timeout": args.timeout,
        "max_retries": args.retries,
        "db_path": args.db,
    }
    settings = dataclasses.replace(
        settings, **{k: v for k, v in overrides.items() if v is not None}
    )

    if not settings.api_key:
        logger.error("OPENAI_API_KEY is not set")
        return 2

    try:
        prompts = load_prompts(args.input, args.prompt)
    except (OSError, ValueError) as exc:
        logger.error("Failed to load prompts: %s", exc)
        return 2

    if not prompts:
        logger.error("No prompts provided. Use --input or --prompt")
        return 2

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    db = Database(settings.db_path)
    logger.info(
        "run=%s tasks=%d workers=%d model=%s db=%s",
        run_id,
        len(prompts),
        min(settings.concurrency, len(prompts)),
        settings.model,
        settings.db_path,
    )

    try:
        results, wall_time = asyncio.run(run_engine(prompts, settings, db, run_id))
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 130
    finally:
        db.close()

    print_summary(results, wall_time, run_id)
    return 0 if results and all(r.status == STATUS_SUCCESS for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
