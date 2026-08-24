# Distributed Task Scheduler & Worker Pool System

[![CI](https://github.com/Khanrukku/distributed-task-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/Khanrukku/distributed-task-scheduler/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Redis](https://img.shields.io/badge/Redis-7-DC382D)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13-FF6600)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688)
![License](https://img.shields.io/badge/License-MIT-green)

A distributed task scheduling and worker-pool system built with **Python, FastAPI, RabbitMQ, Redis, and Docker**, designed to explore real-world distributed systems concepts including **concurrency, task prioritization, fault tolerance, worker coordination, retries, deduplication, and horizontal scaling**.

---

## 🚀 What Does This Project Do?

Applications often need to execute work that should not block the main application.

Examples include:

- processing large datasets
- sending notifications
- generating reports
- running background computations
- processing uploaded files
- executing AI/ML workloads

Instead of executing these jobs directly inside the API server, this project sends them to a **distributed task queue**.

RabbitMQ delivers the tasks to available workers, Redis maintains task and worker state, and multiple workers can process tasks concurrently.

```text
Client
   │
   │ HTTP Request
   ▼
FastAPI
   │
   │ Publish Task
   ▼
RabbitMQ
   │
   ├──────────────┬──────────────┐
   ▼              ▼              ▼
Worker 1       Worker 2       Worker N
   │              │              │
   └──────────────┴──────────────┘
                  │
                  ▼
                Redis
        Task & Worker State
```

---

# 🏗️ Architecture

```text
┌─────────────┐       HTTP        ┌──────────────────┐
│   Client    │ ─────────────────▶│     FastAPI      │
└─────────────┘                   │    REST API      │
                                  └────────┬─────────┘
                                           │
                                           │ publish
                                           ▼
                                  ┌──────────────────┐
                                  │     RabbitMQ     │
                                  │                  │
                                  │ Priority Queue   │
                                  │ Persistent Msgs  │
                                  │ Dead Lettering   │
                                  └────────┬─────────┘
                                           │
                                  competing consumers
                                           │
                       ┌───────────────────┼───────────────────┐
                       ▼                   ▼                   ▼
                  ┌─────────┐         ┌─────────┐         ┌─────────┐
                  │Worker 1 │         │Worker 2 │   ...   │Worker N │
                  └────┬────┘         └────┬────┘         └────┬────┘
                       │                   │                   │
                       └───────────────────┼───────────────────┘
                                           │
                                           ▼
                                  ┌──────────────────┐
                                  │      Redis       │
                                  │                  │
                                  │ Task State       │
                                  │ Worker Registry  │
                                  │ Deduplication    │
                                  │ Locks            │
                                  │ Priority State   │
                                  └──────────────────┘
```

---

# ✨ Key Features

### ⚡ Concurrent Task Execution

Workers use Python's `asyncio` primitives to execute multiple tasks concurrently while maintaining a configurable concurrency limit.

### 🎯 Priority-Based Scheduling

Tasks can be submitted with different priority levels:

- `LOW`
- `MEDIUM`
- `HIGH`

RabbitMQ priority queues allow higher-priority jobs to be delivered before lower-priority jobs waiting in the queue.

### 🔁 Automatic Retry

Failed tasks can be retried using exponential backoff.

```text
2s → 4s → 8s
```

This reduces pressure on downstream systems when failures are temporary.

### ☠️ Dead-Letter Queue

Tasks that cannot be processed successfully after the configured retry policy can be routed to a dead-letter queue for later inspection.

### 🧠 Task Deduplication

Redis-backed atomic operations help prevent duplicate task submissions from creating multiple task records.

### 💓 Worker Heartbeats

Workers periodically update their state in Redis.

Worker records use TTL-based expiration so stale workers can be detected and cleaned up.

### 🔒 Distributed Locking

Redis atomic lock operations provide a foundation for protecting distributed critical sections.

### 📈 Horizontal Scaling

Additional worker processes or containers can be started to increase processing capacity.

All workers compete for tasks from the shared RabbitMQ queue.

### 🐳 Containerized Infrastructure

The application stack can be started through Docker Compose, including:

- API
- Redis
- RabbitMQ
- worker runtime

### ✅ Continuous Integration

GitHub Actions automatically runs the project's test suite whenever changes are pushed.

---

# 🧠 Distributed Systems Concepts Demonstrated

| Concept | Implementation |
|---|---|
| **Asynchronous processing** | `asyncio` based worker runtime |
| **Concurrency control** | `asyncio.Semaphore` |
| **State synchronization** | `asyncio.Lock` |
| **Distributed coordination** | Redis atomic operations |
| **Task deduplication** | Redis `SETNX` |
| **Priority scheduling** | RabbitMQ priority queues + Redis sorted-set state |
| **Reliable messaging** | RabbitMQ persistent messages |
| **Failure handling** | Retry strategy with exponential backoff |
| **Dead-letter handling** | RabbitMQ dead-letter queue |
| **Worker health tracking** | Redis heartbeat records with TTL |
| **Competing consumers** | Multiple workers consume the same task queue |
| **Backpressure** | RabbitMQ configurable prefetch |
| **Horizontal scalability** | Multiple worker processes/containers |

---

# 📂 Project Structure

```text
distributed-task-scheduler/
│
├── api/
│   └── main.py
│
├── models/
│   └── task.py
│
├── scheduler/
│   └── queue_manager.py
│
├── worker/
│   ├── worker.py
│   ├── pool.py
│   └── task_executor.py
│
├── utils/
│   └── redis_store.py
│
├── tests/
│   └── test_scheduler.py
│
├── screenshots/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── docker-compose.yml
├── Dockerfile
├── run_worker.py
├── requirements.txt
└── README.md
```

### Important Components

**`api/main.py`**

Provides the REST API for submitting tasks, retrieving task state, listing tasks, and inspecting workers.

**`scheduler/queue_manager.py`**

Handles RabbitMQ connections, task publishing, priority delivery, acknowledgements, concurrent message dispatch, and dead-letter routing.

**`worker/worker.py`**

Executes tasks while handling concurrency limits, worker state, timeouts, failures, and retries.

**`worker/pool.py`**

Creates and manages multiple worker instances.

**`worker/task_executor.py`**

Provides the pluggable task-handler registry used to dispatch different task types.

**`utils/redis_store.py`**

Maintains task state, worker information, deduplication state, distributed locks, and priority-related data.

---

# 🔄 Task Lifecycle

A typical task moves through the following flow:

```text
Client submits task
        │
        ▼
FastAPI validates request
        │
        ▼
Task state stored in Redis
        │
        ▼
Task published to RabbitMQ
        │
        ▼
Available worker receives task
        │
        ▼
Task marked RUNNING
        │
        ▼
Task handler executes
        │
        ├──────── SUCCESS ────────▶ COMPLETED
        │
        └──────── FAILURE
                    │
                    ▼
                  Retry
                    │
              retries exhausted
                    │
                    ▼
              DEAD / DLQ
```

---

# 📊 Task States

Tasks can move through several states:

```text
PENDING
   │
   ▼
QUEUED
   │
   ▼
RUNNING
   │
   ├──────────────▶ COMPLETED
   │
   └── failure ───▶ QUEUED
                         │
                         │ retry
                         ▼
                      RUNNING
                         │
                    retries exhausted
                         ▼
                        DEAD
```

---

# 🎯 Priority Scheduling

The scheduler supports three priority levels:

| Priority | Value |
|---|---:|
| LOW | 1 |
| MEDIUM | 5 |
| HIGH | 10 |

RabbitMQ is configured as a priority queue using:

```text
x-max-priority = 10
```

Higher-priority queued tasks can therefore be selected before lower-priority tasks.

---

# ⚙️ Concurrency Model

Each worker has a configurable concurrency limit.

A semaphore prevents a worker from exceeding its available task slots:

```python
async with self._semaphore:
    await self._run_task(task)
```

The worker pool can create multiple worker instances:

```text
Worker Pool
│
├── Worker 1
│   ├── Task Slot
│   └── Task Slot
│
├── Worker 2
│   ├── Task Slot
│   └── Task Slot
│
└── Worker N
```

The approximate processing capacity of one runtime is:

```text
number_of_workers × concurrency_per_worker
```

RabbitMQ prefetch can be configured to provide enough outstanding work for the available processing capacity.

---

# 🔁 Fault Tolerance & Retry Strategy

Temporary failures are handled using exponential backoff.

For the default three-retry configuration:

| Retry | Delay |
|---|---:|
| Retry 1 | 2 seconds |
| Retry 2 | 4 seconds |
| Retry 3 | 8 seconds |
| Exhausted | Dead-letter handling |

This prevents failed jobs from being retried continuously without delay.

Tasks also have an execution timeout to prevent individual jobs from running indefinitely.

---

# 💓 Worker Health Monitoring

Every worker registers itself in Redis.

Workers periodically refresh a heartbeat record:

```text
worker:{worker_id}
```

The worker record expires if heartbeats stop.

This provides a simple failure-detection mechanism for workers that terminate unexpectedly.

---

# 🔒 Task Deduplication

Task creation uses an atomic Redis operation:

```python
SETNX task:{task_id}
```

Only the first write for a task identifier succeeds.

This helps prevent repeated submissions of the same task ID from creating duplicate task state.

---

# 🧩 Pluggable Task Handlers

Task execution is separated from task scheduling.

Handlers are registered by task name:

```python
@register("echo")
async def handle_echo(payload):
    return payload
```

The worker dynamically selects the appropriate handler:

```text
Task
 │
 │ name = "compute"
 ▼
Task Registry
 │
 ▼
compute handler
```

This makes new task types easy to add without modifying the scheduling infrastructure.

---

# 🧪 Built-In Task Types

| Task | Example Payload | Purpose |
|---|---|---|
| `echo` | `{"msg": "hello"}` | Returns the supplied payload |
| `compute` | `{"n": 1000000}` | CPU-bound computation example |
| `sleep` | `{"duration": 2}` | Simulates asynchronous work |
| `fail` | `{"reason": "test"}` | Exercises failure/retry behavior |

---

# 🐳 Running the Project

## Prerequisites

Install:

- Docker
- Docker Compose
- Git

---

## Clone the Repository

```bash
git clone https://github.com/Khanrukku/distributed-task-scheduler.git
cd distributed-task-scheduler
```

---

## Start the Stack

```bash
docker compose up --build
```

The stack starts the project's configured services, including Redis, RabbitMQ, the API, and worker runtime.

---

# 🌐 API

Once the application is running, FastAPI's interactive documentation is available at:

```text
http://localhost:8000/docs
```

## Submit a Task

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "compute",
    "payload": {
      "n": 1000000
    },
    "priority": 10
  }'
```

## Check Task Status

```bash
curl http://localhost:8000/tasks/{task_id}
```

## List Tasks

```bash
curl http://localhost:8000/tasks
```

## List Active Workers

```bash
curl http://localhost:8000/workers
```

---

# 📸 Screenshots

## RabbitMQ Management Dashboard

RabbitMQ exposes queue state and message activity through its management interface.

![RabbitMQ Dashboard](screenshots/rabbitmq.png)

## FastAPI Swagger UI

Interactive API documentation generated automatically by FastAPI.

![Swagger UI](screenshots/swagger.png)

## Docker Stack

Redis, RabbitMQ, API, and worker services running through Docker Compose.

![Docker Running](screenshots/docker.png)

---

# 🧪 Testing

Run the test suite locally:

```bash
pytest -q
```

The repository also uses GitHub Actions to run automated tests on pushes and pull requests.

The current workflow status is shown by the CI badge at the top of this README.

---

# 🛠️ Tech Stack

| Technology | Responsibility |
|---|---|
| **Python 3.12** | Application and asynchronous worker runtime |
| **FastAPI** | REST API |
| **RabbitMQ** | Message broker and task delivery |
| **Redis** | Task state, worker registry, locks and deduplication |
| **Pydantic v2** | Validation and serialization |
| **asyncio** | Asynchronous execution and concurrency |
| **Docker** | Service containerization |
| **Docker Compose** | Multi-service local environment |
| **pytest** | Automated testing |
| **GitHub Actions** | Continuous integration |

---

# 💡 Engineering Decisions

### Why RabbitMQ?

RabbitMQ separates task producers from task consumers and provides durable messaging, acknowledgement semantics, priority queues, and dead-letter capabilities.

### Why Redis?

Redis provides fast shared state accessible by multiple processes and supports atomic primitives useful for task state, worker heartbeats, deduplication, and distributed coordination.

### Why AsyncIO?

Many scheduler operations involve waiting on Redis, RabbitMQ, or other I/O. Async execution allows workers to make progress on other operations while waiting.

### Why Separate the API and Workers?

The API's responsibility is accepting and validating requests.

Workers are responsible for executing background jobs.

Separating them allows the two components to scale independently.

---

# 📈 Scaling

The architecture supports scaling the worker layer independently of the API.

```text
                    RabbitMQ
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
      Worker A     Worker B     Worker C
          │            │            │
          └────────────┼────────────┘
                       ▼
                     Redis
```

Additional worker processes or containers can join the same queue and participate as competing consumers.

---

# 🔮 Future Improvements

Possible production-hardening improvements include:

- delayed retry queues instead of worker-side backoff waits
- stronger distributed-lock ownership semantics
- idempotency keys at the API boundary
- task cancellation
- scheduled/delayed tasks
- metrics with Prometheus
- Grafana dashboards
- structured logging
- distributed tracing
- authentication and authorization
- rate limiting
- Kubernetes deployment
- autoscaling based on queue depth
- stronger delivery/idempotency guarantees across failure boundaries

These are intentionally listed as future improvements rather than claimed as currently implemented functionality.

---

# 🎓 What This Project Demonstrates

This project was built to go beyond a basic CRUD application and explore how asynchronous distributed systems coordinate work.

It demonstrates practical understanding of:

- producer-consumer architecture
- message brokers
- task queues
- worker pools
- asynchronous programming
- concurrency control
- distributed state
- task prioritization
- retries and failure handling
- dead-letter queues
- worker health tracking
- horizontal scalability
- containerized services
- automated CI testing

---

# 👩‍💻 Author

**Rukaiya Khan**

Software Engineering • Distributed Systems • Cloud • AI/ML

GitHub: [@Khanrukku](https://github.com/Khanrukku)

LinkedIn: [Rukaiya Khan](https://www.linkedin.com/in/rukaiya-khan-a68767315/)

---

⭐ If you find this project useful or interesting, consider starring the repository.
