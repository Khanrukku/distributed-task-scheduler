# Distributed Task Scheduler & Worker Pool System

A production-grade distributed task scheduling system built with Python, demonstrating core distributed systems concepts: **concurrency**, **synchronization**, **fault tolerance**, and **horizontal scalability**.

## Architecture

```
┌─────────────┐     HTTP      ┌──────────────────┐
│   Client    │ ─────────────▶│   FastAPI (API)  │
└─────────────┘               └────────┬─────────┘
                                       │ publish
                              ┌────────▼─────────┐
                              │    RabbitMQ       │◀── Dead-Letter Queue
                              │  (Task Queue)     │
                              └────────┬─────────┘
                        competing      │ consume
                        consumers ┌────▼──────────────────────┐
                                  │        Worker Pool         │
                                  │  ┌──────┐  ┌──────┐       │
                                  │  │  W1  │  │  W2  │  ...  │
                                  │  └──────┘  └──────┘       │
                                  └───────────────────────────┘
                                             │ state
                              ┌──────────────▼──────────────┐
                              │           Redis              │
                              │  Task store · Worker registry│
                              │  Distributed locks · Priority│
                              └─────────────────────────────┘
```

## Key Distributed Systems Concepts Implemented

| Concept | Implementation |
|---|---|
| **Concurrency** | `asyncio.Semaphore` limits concurrent task slots per worker |
| **Synchronization** | `asyncio.Lock` for thread-safe worker state mutations |
| **Distributed locking** | Redis `SETNX` for leader election and critical sections |
| **Task deduplication** | Redis `SETNX` prevents duplicate task submissions |
| **Fault tolerance** | Retry with exponential backoff (2s, 4s, 8s...) |
| **Dead-letter queue** | Tasks exhausting retries routed to DLQ via RabbitMQ DLX |
| **Competing consumers** | Multiple workers consume the same queue — fair dispatch |
| **Heartbeat / failure detection** | Workers send Redis TTL-based heartbeats; dead workers auto-removed |
| **Priority scheduling** | Redis sorted set (ZADD/ZPOPMAX) for O(log N) priority ordering |
| **Horizontal scaling** | Run N worker containers — each competes on the same queue |

## Project Structure

```
task_scheduler/
├── api/
│   └── main.py              # FastAPI REST API
├── models/
│   └── task.py              # Task & Worker data models (Pydantic)
├── scheduler/
│   └── queue_manager.py     # RabbitMQ producer/consumer, DLQ
├── worker/
│   ├── worker.py            # Worker node — heartbeat, retry, concurrency
│   ├── pool.py              # Worker pool manager
│   └── task_executor.py     # Pluggable task handler registry
├── utils/
│   └── redis_store.py       # Redis store — atomic ops, locks, registry
├── tests/
│   └── test_scheduler.py    # Unit tests — concurrency, retry, deduplication
├── docker-compose.yml       # Full stack: Redis + RabbitMQ + API + Workers
├── Dockerfile
├── run_worker.py            # Worker pool entrypoint
└── requirements.txt
```

## Quick Start

### With Docker (recommended)

```bash
# Clone and start the full stack
git clone https://github.com/Khanrukku/distributed-task-scheduler
cd distributed-task-scheduler

docker-compose up --build
```

This starts:
- **Redis** on port 6379
- **RabbitMQ** on port 5672 (management UI: http://localhost:15672)
- **API** on port 8000 (docs: http://localhost:8000/docs)
- **2 worker containers** × 4 workers each = 8 concurrent workers

### Without Docker

```bash
# Start Redis and RabbitMQ separately, then:
pip install -r requirements.txt
cp .env.example .env

# Terminal 1 — API
uvicorn api.main:app --reload

# Terminal 2 — Workers
python run_worker.py
```

## API Usage

### Submit a task
```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"name": "compute", "payload": {"n": 1000000}, "priority": 10}'
```

### Check task status
```bash
curl http://localhost:8000/tasks/{task_id}
```

### List all running workers
```bash
curl http://localhost:8000/workers
```

### Available task types

| Task | Payload | Description |
|---|---|---|
| `echo` | `{"msg": "..."}` | Returns payload — integration testing |
| `compute` | `{"n": 1000}` | CPU-bound sum(range(n)) |
| `sleep` | `{"duration": 2}` | I/O simulation |
| `fail` | `{"reason": "..."}` | Always fails — tests retry/DLQ logic |

## Running Tests

```bash
pytest tests/ -v
```

## Concurrency Deep Dive

Each worker uses `asyncio.Semaphore(max_concurrent=2)` to limit parallel task slots:

```python
async with self._semaphore:          # blocks if 2 tasks already running
    asyncio_task = asyncio.create_task(self._run_task(task))
    await asyncio_task
```

Worker state mutations (status, task count) are protected by `asyncio.Lock`:

```python
async with self._lock:
    self._info.status = "busy"
    await self._store.update_worker(self._info)
```

Distributed task deduplication uses Redis atomic `SETNX`:

```python
await pipe.setnx(key, serialized)    # only succeeds if key doesn't exist
```

## Fault Tolerance

Tasks failing are retried with exponential backoff:
- Retry 1 → wait 2s
- Retry 2 → wait 4s
- Retry 3 → wait 8s
- Exhausted → routed to dead-letter queue

Workers that crash are automatically removed from the registry when their Redis heartbeat TTL expires (30s).

## Tech Stack

- **Python 3.12** — async/await, asyncio primitives
- **FastAPI** — REST API layer
- **Redis** — task store, distributed locks, worker registry, priority queuing
- **RabbitMQ** — reliable message delivery, dead-letter exchange
- **Docker** — containerisation and horizontal scaling
- **Pydantic v2** — data validation and serialisation
- **pytest-asyncio** — async unit testing
