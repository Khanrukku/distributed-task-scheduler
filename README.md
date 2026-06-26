# Distributed Task Scheduler & Worker Pool System

[![CI](https://github.com/Khanrukku/distributed-task-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/Khanrukku/distributed-task-scheduler/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Redis](https://img.shields.io/badge/Redis-7-DC382D)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13-FF6600)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688)
![License](https://img.shields.io/badge/License-MIT-green)

A **production-grade distributed task scheduling system** built with Python, demonstrating core distributed systems concepts: **concurrency**, **synchronization**, **fault tolerance**, and **horizontal scalability**.

Built as part of a portfolio project to demonstrate real-world distributed systems engineering.

---

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

---

## Key Distributed Systems Concepts Implemented

| Concept | Implementation |
|---|---|
| **Concurrency** | `asyncio.Semaphore` limits concurrent task slots per worker |
| **Synchronization** | `asyncio.Lock` for thread-safe worker state mutations |
| **Distributed locking** | Redis `SETNX` for leader election and critical sections |
| **Task deduplication** | Redis `SETNX` prevents duplicate task submissions atomically |
| **Fault tolerance** | Retry with exponential backoff (2s → 4s → 8s) |
| **Dead-letter queue** | Tasks exhausting retries routed to DLQ via RabbitMQ DLX |
| **Competing consumers** | Multiple workers consume the same queue — fair dispatch |
| **Heartbeat / failure detection** | Workers send Redis TTL-based heartbeats; dead workers auto-removed |
| **Priority scheduling** | Redis sorted set (ZADD/ZPOPMAX) for O(log N) priority ordering |
| **Horizontal scaling** | Run N worker containers — each competes on the same queue |

---

## Project Structure

```
task_scheduler/
├── api/
│   └── main.py              # FastAPI REST API — submit, list, status, workers
├── models/
│   └── task.py              # Task & Worker data models (Pydantic v2)
├── scheduler/
│   └── queue_manager.py     # RabbitMQ producer/consumer, DLQ, manual acks
├── worker/
│   ├── worker.py            # Worker node — heartbeat, retry, concurrency control
│   ├── pool.py              # Worker pool manager — N concurrent workers
│   └── task_executor.py     # Pluggable task handler registry
├── utils/
│   └── redis_store.py       # Redis store — atomic ops, locks, priority queue
├── tests/
│   └── test_scheduler.py    # Unit tests — concurrency, retry, deduplication
├── .github/
│   └── workflows/
│       └── ci.yml           # GitHub Actions CI — auto test on every push
├── docker-compose.yml       # Full stack: Redis + RabbitMQ + API + Workers
├── Dockerfile
├── run_worker.py            # Worker pool entrypoint
└── requirements.txt
```

---

## Screenshots

### RabbitMQ Dashboard — Live Queues Running
> Both `task_queue` (with TTL + DLX) and `dead_letter` queue active and running.

![RabbitMQ Dashboard](screenshots/rabbitmq.png)

### API — Swagger UI
> Interactive REST API at `http://localhost:8000/docs`

![Swagger UI](screenshots/swagger.png)

### Docker — Full Stack Running
> API + Worker Pool + Redis + RabbitMQ all running via `docker-compose up --build`

![Docker Running](screenshots/docker.png)

---

## Quick Start

### With Docker (recommended)

```bash
git clone https://github.com/Khanrukku/distributed-task-scheduler
cd distributed-task-scheduler

docker-compose up --build
```

This starts:
- **Redis** on port `6379`
- **RabbitMQ** on port `5672` — management UI at `http://localhost:15672` (guest/guest)
- **API** on port `8000` — interactive docs at `http://localhost:8000/docs`
- **2 worker containers** × 4 workers each = **8 concurrent workers total**

---

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

### List all tasks
```bash
curl http://localhost:8000/tasks
```

### List active workers
```bash
curl http://localhost:8000/workers
```

---

## Available Task Types

| Task | Payload | Description |
|---|---|---|
| `echo` | `{"msg": "hello"}` | Returns payload — integration testing |
| `compute` | `{"n": 1000000}` | CPU-bound computation — sum(range(n)) |
| `sleep` | `{"duration": 2}` | I/O simulation |
| `fail` | `{"reason": "test"}` | Always fails — tests retry and dead-letter logic |

---

## Concurrency Deep Dive

Each worker uses `asyncio.Semaphore` to enforce max concurrent task slots:

```python
async with self._semaphore:          # blocks if slots full
    asyncio_task = asyncio.create_task(self._run_task(task))
    await asyncio_task
```

Worker state mutations are protected by `asyncio.Lock`:

```python
async with self._lock:
    self._info.status = "busy"
    await self._store.update_worker(self._info)
```

Distributed task deduplication uses Redis atomic `SETNX`:

```python
await pipe.setnx(key, serialized)    # only succeeds if key doesn't exist
```

---

## Fault Tolerance

Tasks that fail are retried with **exponential backoff**:

| Retry | Wait |
|---|---|
| 1st | 2 seconds |
| 2nd | 4 seconds |
| 3rd | 8 seconds |
| Exhausted | → Dead-letter queue |

Workers that crash are **automatically removed** from the registry when their Redis heartbeat TTL expires (30 seconds).

---

## Tech Stack

| Technology | Purpose |
|---|---|
| **Python 3.12** | Async/await, asyncio primitives, concurrency |
| **FastAPI** | REST API layer with automatic OpenAPI docs |
| **Redis 7** | Task store, distributed locks, priority queue, worker registry |
| **RabbitMQ 3.13** | Reliable message delivery, dead-letter exchange, manual acks |
| **Docker + Compose** | Containerisation and horizontal scaling |
| **Pydantic v2** | Data validation and serialisation |
| **pytest-asyncio** | Async unit testing |
| **GitHub Actions** | CI — automated testing on every push |

---

## Author

**Rukaiya Khan**
- GitHub: [@Khanrukku](https://github.com/Khanrukku)
- LinkedIn: [linkedin.com/in/rukaiyakhan](https://linkedin.com/in/rukaiyakhan)
- Email: khanrukiya2810@gmail.com
