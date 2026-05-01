# Airflow in this repo (`dbt-tenant-controller`)

This document explains **what Apache Airflow is**, the **core concepts you need**, and **how it’s wired in *this* repository** so you can start extending it confidently.

> Repo reality check: Airflow is currently used as a **workflow orchestrator** that triggers dbt operations by calling your **FastAPI control plane** over HTTP.
>
> In other words: Airflow does *not* run dbt directly here (no BashOperator/dbt CLI inside the worker). It calls an API endpoint that runs `dbt ...`.

---

## 1) What Airflow is (the mental model)

Airflow is a **workflow orchestration platform**.

You define workflows as code (“**DAGs**”), and Airflow:

- decides **when** they should run (schedules / triggers)
- decides **what** should run next (dependencies)
- executes tasks somewhere (workers/executors)
- tracks **state** and **history** in a metadata database
- gives you a UI to trigger runs, inspect logs, retry, etc.

Airflow is **not** a streaming system and not a generic compute engine. It’s best at coordinating *steps* and handling retries/visibility.

---

## 2) Key concepts you need (minimal-but-real)

### DAG
A **Directed Acyclic Graph**: a set of tasks + dependencies.

In code, a DAG is usually created like:

```py
with DAG(...) as dag:
    task_a >> task_b
```

### Task / Operator
A **task** is one node in the DAG. Operators are the “task templates”.

In this repo you use:

- `SimpleHttpOperator` from `apache-airflow-providers-http`

### DagRun and TaskInstance

- **DagRun**: one execution of a DAG (manual trigger or schedule)
- **TaskInstance**: one execution of a specific task within that DagRun

If you click “Run” in the UI, you’re creating a DagRun. Each task becomes a TaskInstance with its own state and logs.

### Scheduler
The scheduler:

- parses DAG files
- decides when DAGs should create runs
- queues tasks for execution

### Executor / Worker
The **executor** defines how tasks are executed.

In this repo you configured:

- **CeleryExecutor** (distributed workers)

With CeleryExecutor, tasks are sent to a queue (Redis here), and one of your worker containers picks them up.

### Metadata database
Airflow stores:

- DAG definitions it has seen
- schedules
- run history and task state
- connections, variables, etc.

In this repo the metadata DB is a **Postgres container** dedicated to Airflow (not your analytics warehouse).

### Connection (`conn_id`)
Many operators reference external systems via a **Connection** identified by a `conn_id`.

Your DAG uses `http_conn_id="fastapi"`, which means you must create an Airflow connection named **`fastapi`** so tasks know where the FastAPI service lives.

---

## 3) Your current Airflow deployment (from `docker-compose.yml`)

Airflow is deployed as several containers:

### Core Airflow services

From `docker-compose.yml`:

- `airflow-webserver` – serves the UI (mapped to **http://localhost:8081**)
- `airflow-scheduler` – schedules DAGs and queues tasks
- `airflow-worker` – executes tasks (Celery worker)
- `airflow-init` – one-time initialization (db migration + create admin user)

### Supporting services

- `airflow-db` – Postgres 15: the **Airflow metadata DB**
- `redis` – Redis 7: **Celery broker** (task queue)

### Executor choice
Your Compose sets:

```yaml
AIRFLOW__CORE__EXECUTOR: CeleryExecutor
```

So the runtime flow is:

1. Scheduler decides a task should run
2. Scheduler sends the task to Redis (broker)
3. Worker container picks it up and runs it
4. State is written to Postgres (`airflow-db`)
5. Webserver reads state from Postgres and displays it

### Shared volumes
You mount:

```yaml
./dags:/opt/airflow/dags
tenant_projects:/opt/airflow/tenants
/var/run/docker.sock:/var/run/docker.sock
```

Implications:

- **DAGs are deployed by bind-mounting your repo’s `./dags` folder** into all Airflow containers.
  - Add/edit a file under `dags/` and the scheduler/webserver/worker see it.
- You also mount the same named volume used by FastAPI (`tenant_projects`) into Airflow.
  - Currently, your DAG does not read/write this path.
  - It’s likely there for future work (e.g., run dbt locally from the worker, generate artifacts, inspect per-tenant project dirs).
- Docker socket is mounted.
  - This is powerful (lets containers talk to Docker) but also a security risk in production.
  - It suggests you might later use DockerOperator or run ephemeral containers for dbt.

### Secrets / keys
Compose pins `FERNET_KEY` and `WEBSERVER__SECRET_KEY`.

- `FERNET_KEY` encrypts sensitive values stored in the metadata DB (connections/passwords).
- Webserver secret key signs sessions.

For a PoC this is fine; for production, treat these as secrets.

### Networking
All services attach to the external network `backend`:

```yaml
networks:
  backend:
    external: true
```

So service names are DNS names **within that docker network**. For example:

- the FastAPI service is reachable at `http://fastapi:8000` from Airflow containers.

---

## 4) The DAG you have today (`dags/dbt_tenant_pipeline.py`)

### What it does
You have one DAG named `dbt_tenant_pipeline`.

Key properties:

```py
dag_id="dbt_tenant_pipeline"
schedule=None
catchup=False
```

Meaning:

- It has **no schedule** (manual trigger only) unless you change it.
- No backfilling (“catchup”).

### Tasks
All tasks are `SimpleHttpOperator` calls to FastAPI:

1) `dbt_deps` → POST `/run-dbt/<TENANT>/deps`

2) `dbt_staging` → POST `/run-dbt/<TENANT>/run --select staging`

3) `dbt_marts` → POST `/run-dbt/<TENANT>/run --select marts`

4) `dbt_test` → POST `/run-dbt/<TENANT>/test`

Dependencies:

```py
dbt_deps >> dbt_staging >> dbt_marts >> dbt_test
```

### Important nuance: the endpoint path contains spaces
Your DAG builds endpoints like:

```py
endpoint=f"/run-dbt/{TENANT}/run --select staging"
```

This means the URL path contains spaces. Most HTTP clients will URL-encode spaces (`%20`), but it’s an unusual pattern and can be brittle.

In FastAPI you defined:

```py
@app.post("/run-dbt/{tenant}/{command}")
def run_dbt(tenant: str, command: str):
    ...
    cmd = f"cd {project_path} && dbt {command}"
```

This works because FastAPI path params can include spaces when URL-encoded, but it’s typically safer to pass the command via:

- query string (`/run-dbt/<tenant>/run?select=staging`)
- or request body (`{"subcommand":"run","args":["--select","staging"]}`)

You don’t have to change it right now, but keep this in mind before you expand command variety.

---

## 5) How an Airflow run flows end-to-end in this repo

Let’s follow a single manual trigger.

### Step 0: You trigger the DAG
In the Airflow UI (webserver), you click “Trigger DAG”.

Airflow creates a **DagRun** record in the metadata DB (`airflow-db`).

### Step 1: Scheduler enqueues the first task
Scheduler sees a runnable task (`dbt_deps`) and publishes it to **Redis**.

### Step 2: Worker executes the task
The Celery worker:

- picks up the task message from Redis
- performs the HTTP request to FastAPI (`http_conn_id="fastapi"`)
- stores the task result (status/logs) in the metadata DB

### Step 3: FastAPI runs dbt (inside the FastAPI container)
FastAPI’s `/run-dbt/...` endpoint runs:

```bash
cd /app/tenants/<tenant> && dbt <command>
```

Important: this happens in the **FastAPI container**, because that is where `subprocess.run()` executes.

That container has:

- the generated tenant project under `/app/tenants/<tenant>`
- Python deps from `requirements.txt` including `dbt-postgres`

So the web request is essentially “remote-controlling” dbt.

### Step 4: Downstream tasks run
When `dbt_deps` is marked success, scheduler queues `dbt_staging`, and so on.

---

## 6) Required setup: create the `fastapi` Airflow connection

Your DAG references:

```py
http_conn_id="fastapi"
```

Airflow won’t magically know what that is. You must create it once.

### Option A (UI) — recommended for now

1. Open Airflow UI: http://localhost:8081
2. Login (created by `airflow-init`):
   - username: `airflow`
   - password: `airflow`
3. Admin → Connections → “+”
4. Create:
   - Connection Id: `fastapi`
   - Connection Type: `HTTP`
   - Host: `http://fastapi`
   - Port: `8000`

After that, your `SimpleHttpOperator` tasks should be able to call your API.

### Option B (env var) — useful for automation
Airflow supports defining connections via environment variables:

```bash
AIRFLOW_CONN_FASTAPI=http://fastapi:8000
```

This repo does not currently set that in compose, but you can add it to the Airflow services if you want a fully reproducible bootstrap.

---

## 7) Local dev workflows (what you’ll do day-to-day)

### Start everything

Your compose relies on an external `backend` network:

```bash
docker network create backend
```

Then:

```bash
docker compose up -d --build
```

### Initialize Airflow (first run only)
Run once:

```bash
docker compose up airflow-init
```

Then start the rest (or just `docker compose up -d` again).

### Trigger your DAG

1. UI: http://localhost:8081
2. Trigger `dbt_tenant_pipeline`
3. Watch task logs

### Debug logs quickly

```bash
docker logs -f airflow-scheduler
docker logs -f airflow-worker
docker logs -f airflow-webserver
```

### “Why isn’t my DAG showing up?” checklist

- File is under `./dags/`
- Python imports are valid inside the Airflow image
- scheduler is running and healthy
- the DAG didn’t error during parsing (check scheduler logs)

---

## 8) Common gotchas / constraints in this exact design

### Gotcha A: dbt is being executed in the FastAPI container
Pros:

- Simple to understand and demo
- No need to install dbt in Airflow

Cons:

- Your Airflow tasks are now dependent on an HTTP service.
- If dbt takes a long time, you’re holding an HTTP request open.
- Retries can re-run the same dbt command unless you add idempotency.

If you evolve this, a typical direction is:

- Airflow triggers a **job** (queue/job runner) and polls for status
- or Airflow runs dbt itself (KubernetesPodOperator, DockerOperator, BashOperator)

### Gotcha B: single hard-coded tenant in the DAG
The DAG has:

```py
TENANT = "customer_000"
```

That’s fine for the PoC, but if you want multi-tenant scheduling you’ll likely move towards:

- DAG params (`dagrun.conf`) to pass tenant at trigger-time
- dynamically mapped tasks (Airflow 2.3+)
- or generating one DAG per tenant (works but can get noisy)

### Gotcha C: external network `backend` must exist
If `backend` network doesn’t exist, compose will fail.

### Gotcha D: No explicit healthchecks / readiness gates
Compose does not define healthchecks; `depends_on` only controls startup order.

If Airflow starts before Postgres is ready you can see transient errors.

### Gotcha E: docker.sock mount is “root level” access
If you later use DockerOperator, this becomes convenient.

But in production it’s essentially privileged access. Prefer KubernetesPodOperator / job runners or a restricted sidecar pattern.

---

## 9) How to extend from here (practical next steps)

If your goal is “add more capabilities”, here are the most leverage-heavy improvements for this repo’s direction.

### 1) Make the DAG tenant-parameterized
Change the DAG to accept a tenant at runtime:

- Use `params={"tenant": "customer_000"}` and/or
- Read `dag_run.conf.get("tenant")` in a Python callable

Then use that tenant string to build endpoints.

### 2) Stop putting raw dbt commands in the URL path
Move to:

- `/run-dbt/{tenant}` with a JSON body `{ "command": "run", "args": ["--select", "staging"] }`

This will make it much easier to:

- validate allowed commands
- prevent injection risks
- evolve to more complex options (vars, full-refresh, state, etc.)

### 3) Add idempotency + locking around per-tenant operations
You already have a `concurrency_plan.md`. Airflow retries are a feature — but retries require safe operations.

At minimum:

- treat tenant as the concurrency unit
- prevent two runs for same tenant from overlapping

### 4) Decide where dbt should really run
You currently have 3 plausible models:

1. **dbt in FastAPI container** (current)
2. **dbt in Airflow worker** (install dbt + use BashOperator / PythonOperator)
3. **dbt in ephemeral container per run** (DockerOperator / KubernetesPodOperator)

Option 3 is usually the cleanest separation.

---

## 10) Quick reference: where to look in the repo

- Airflow compose/services: `docker-compose.yml`
- DAGs: `dags/dbt_tenant_pipeline.py`
- FastAPI dbt execution endpoint: `app/main.py` (`POST /run-dbt/{tenant}/{command}`)
- Shared tenant project volume: `tenant_projects` (compose volume)
