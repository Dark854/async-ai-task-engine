# async-ai-task-engine

An asynchronous microservice worker that processes a queue of prompts against an LLM API (OpenAI) in parallel, validates every response, and logs full request/response history and execution metrics to SQLite. Ships as a single Docker image.

Built with `asyncio`, `aiohttp`, and `sqlite3`.

## Features

- **Parallel processing** – an `asyncio.Queue` feeds a configurable pool of workers, so N prompts are in flight at once instead of waiting one by one.
- **Resilient HTTP layer** – handles network errors, timeouts, and non-200 statuses; retries `429` and `5xx` responses with exponential backoff and jitter.
- **Response validation** – every completion is checked for a well-formed JSON body and non-empty content before it is marked successful.
- **Structured logging to SQLite** – each task is stored with prompt, response, status (`SUCCESS`, `FAILED`, `ERROR`), HTTP code, attempts, latency, token usage, model, and timestamp.
- **Run summary** – prints success/failure counts, average latency, token totals, and the speedup versus sequential execution.
- **OpenAI-compatible** – point `OPENAI_BASE_URL` at any compatible endpoint.
- **Container-ready** – slim Python 3.11 image, non-root user, persistent volume for the database.

### How it works

```
prompts (file / CLI)
        |
        v
  asyncio.Queue  --->  worker-1 --\
                  --->  worker-2 ---+--> aiohttp --> LLM API
                  --->  worker-N --/         |
                                             v
                            retry / timeout / validation
                                             |
                                             v
                                  SQLite (task_logs table)
```

| Status    | Meaning                                                                  |
|-----------|--------------------------------------------------------------------------|
| `SUCCESS` | HTTP 200 and a valid, non-empty completion                               |
| `FAILED`  | Non-200 HTTP status (after retries) or a response that failed validation |
| `ERROR`   | Network failure or timeout (after retries)                               |

## Quick Start

**Requirements:** Python 3.11+ and an OpenAI API key.

```bash
git clone https://github.com/Dark854/async-ai-task-engine.git
cd async-ai-task-engine

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Create a `.env` file:

```env
OPENAI_API_KEY=sk-your-key
OPENAI_MODEL=gpt-4o-mini
MAX_CONCURRENCY=5
REQUEST_TIMEOUT=30
MAX_RETRIES=3
DB_PATH=tasks.db
```

Create a prompts file (one prompt per line) and run:

```bash
cat > prompts.txt << 'PROMPTS'
Explain asyncio in two sentences.
Give three tips for writing clean Python.
What is the difference between a process and a thread?
Summarize the CAP theorem.
PROMPTS

python app.py --input prompts.txt
```

Inline prompts and overrides work too:

```bash
python app.py -p "Hello!" -p "What is Docker?" --concurrency 10 --model gpt-4o-mini
```

### CLI options

| Option                | Description                                        |
|-----------------------|----------------------------------------------------|
| `-i`, `--input`       | `.txt` (one prompt per line) or `.json` (list of strings / `{"prompt": ...}` objects) |
| `-p`, `--prompt`      | Inline prompt, repeatable                          |
| `-c`, `--concurrency` | Number of parallel workers                         |
| `-m`, `--model`       | Model name                                         |
| `--timeout`           | Request timeout in seconds                         |
| `--retries`           | Maximum retries per task                           |
| `--db`                | SQLite database path                               |

CLI options take precedence over environment variables.

### Inspecting results

```bash
sqlite3 tasks.db "SELECT status, COUNT(*), ROUND(AVG(latency_ms)) AS avg_ms FROM task_logs GROUP BY status;"
sqlite3 tasks.db "SELECT task_id, status, http_status, attempts, latency_ms FROM task_logs ORDER BY id DESC LIMIT 10;"
```

## Docker

Build the image:

```bash
docker build -t async-ai-task-engine .
```

Run with inline prompts (the database is persisted in the `ai_task_data` volume):

```bash
docker run --rm \
  --env-file .env \
  -v ai_task_data:/data \
  async-ai-task-engine -p "Explain asyncio briefly." -p "What is a REST API?"
```

Run with a prompts file:

```bash
docker run --rm \
  --env-file .env \
  -v ai_task_data:/data \
  -v "$(pwd)/prompts.txt:/app/prompts.txt:ro" \
  async-ai-task-engine --input prompts.txt
```

Read the logs from the volume:

```bash
docker run --rm -v ai_task_data:/data --entrypoint python async-ai-task-engine \
  -c "import sqlite3; print(sqlite3.connect('/data/tasks.db').execute('SELECT status, COUNT(*) FROM task_logs GROUP BY status').fetchall())"
```

## Project Structure

```
async-ai-task-engine/
├── app.py
├── requirements.txt
├── Dockerfile
├── README.md
└── .gitignore
```

## License

MIT
