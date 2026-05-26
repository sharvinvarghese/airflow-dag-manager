# 🌪️ Airflow DAG Manager

> A lightweight Python CLI toolkit for interacting with the **Apache Airflow REST API v2** — fetch DAG runs, inspect task instances, and patch task states directly from your terminal.

[![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-v2-017CEE?style=flat-square&logo=apache-airflow&logoColor=white)](https://airflow.apache.org)
[![License](https://img.shields.io/badge/License-MIT-01696f?style=flat-square)](LICENSE)
[![REST API](https://img.shields.io/badge/API-REST%20v2-orange?style=flat-square)](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)

---

## 📋 Overview

This toolkit provides a simple, modular Python interface to the **Airflow REST API v2**, letting you:

- 📦 **List all DAG runs** for any DAG, sorted by most recent first
- 🔍 **Inspect task instance details** including state, operator, and map index
- ✅ **Patch task states** to `success` programmatically (great for recovering stuck tasks)

No UI needed — everything runs from your terminal.

---

## 📁 Project Structure

```
airflow-dag-manager/
├── main.py                  # Interactive CLI entrypoint
├── AirFlowDagFetch.py       # Fetch DAG runs via GET /api/v2/dags/{dag_id}/dagRuns
├── AirFlowTaskFetch.py      # Fetch task instance via GET /api/v2/.../taskInstances/{task_id}
└── AirFlowTaskChanges.py    # Patch task state via PATCH /api/v2/.../taskInstances/{task_id}
```

---

## ⚡ Quickstart

### Prerequisites

- Python 3.8+
- A running Apache Airflow instance (local, Docker, or remote) with the REST API enabled
- `requests` library

### Installation

```bash
git clone https://github.com/sharvinvarghese/airflow-dag-manager.git
cd airflow-dag-manager
pip install requests
```

### Configure Credentials

Open `AirFlowDagFetch.py` and replace the hardcoded credentials:

```python
# Before
auth=("admin", "admin")

# After — use environment variables
import os
auth=(os.environ["AIRFLOW_USER"], os.environ["AIRFLOW_PASSWORD"])
```

Or export them:

```bash
export AIRFLOW_USER=your_username
export AIRFLOW_PASSWORD=your_password
```

### Run

```bash
python main.py
```

You'll be guided through an interactive prompt:

```
Enter DAG name: my_pipeline

Fetching DAG runs...

1. scheduled__2026-05-26T00:00:00+00:00 | State=failed | RunAfter=2026-05-26T00:05:00
2. scheduled__2026-05-25T00:00:00+00:00 | State=success | RunAfter=2026-05-25T00:05:00

Select DAG run number: 1

Enter task_id (example: bb_pipeline): extract_data

Fetching task details...

Task Details
-------------
Task ID: extract_data
State: failed
Operator: PythonOperator
Rendered Map Index: None

Available Map Indexes
1. 0

Select map index number: 1

Triggering PATCH state change...

PATCH Success
{'task_id': 'extract_data', 'state': 'success', ...}
```

---

## 🔧 Module Reference

### `AirFlowDagFetch.py` — `get_dag_runs()`

Fetches all DAG runs for a given DAG, sorted by most recent `run_after`.

```python
from AirFlowDagFetch import get_dag_runs

data = get_dag_runs(
    dag_name="my_pipeline",
    host="http://localhost:8080",  # or your Airflow URL
    limit=50,
    offset=0,
    order_by="id"
)

for run in data["dag_runs"]:
    print(run["dag_run_id"], run["state"])
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dag_name` | `str` | required | DAG ID to query |
| `host` | `str` | `localhost:8080` | Airflow base URL |
| `limit` | `int` | `50` | Max runs to return |
| `offset` | `int` | `0` | Pagination offset |
| `order_by` | `str` | `id` | Sort field |

---

### `AirFlowTaskFetch.py` — `get_task_instance()`

Retrieves details for a single task instance in a specific DAG run.

```python
from AirFlowTaskFetch import get_task_instance

task = get_task_instance(
    dag_name="my_pipeline",
    dag_run_id="manual__2026-05-25T16:17:59.984723+00:00",
    task_id="extract_data"
)

print(task["state"])     # e.g. "failed"
print(task["operator"]) # e.g. "PythonOperator"
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dag_name` | `str` | required | DAG ID |
| `dag_run_id` | `str` | required | Full DAG run ID string |
| `task_id` | `str` | required | Task ID within the DAG |
| `host` | `str` | `localhost:8080` | Airflow base URL |

---

### `AirFlowTaskChanges.py` — `change_task_state()`

Patches a task instance's state to `success` using the Airflow REST API.

```python
from AirFlowTaskChanges import change_task_state

result = change_task_state(
    dag_name="my_pipeline",
    dag_run_id="manual__2026-05-25T16:17:59.984723+00:00",
    task_id="extract_data",
    map_index=0
)

print(result)  # Patched task instance JSON
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dag_name` | `str` | required | DAG ID |
| `dag_run_id` | `str` | required | Full DAG run ID string |
| `task_id` | `str` | required | Task ID to patch |
| `map_index` | `int` | required | Map index (use `0` for non-mapped tasks) |
| `host` | `str` | `localhost:8080` | Airflow base URL |

---

## 💡 Use Cases

### 1. 🛠️ Recovering Stuck or Failed Tasks

When a task fails due to a transient issue (network blip, temporary DB downtime) and you don't want to re-run the entire DAG, use `change_task_state()` to mark it as `success` and let downstream tasks proceed.

```python
from AirFlowTaskChanges import change_task_state

change_task_state(
    dag_name="etl_pipeline",
    dag_run_id="scheduled__2026-05-26T00:00:00+00:00",
    task_id="load_to_warehouse",
    map_index=0
)
```

### 2. 📊 Pipeline Run Auditing

Quickly audit recent DAG runs and their states across your environment without logging into the Airflow UI.

```python
from AirFlowDagFetch import get_dag_runs

runs = get_dag_runs("daily_report", limit=10)["dag_runs"]
for r in runs:
    print(f"{r['dag_run_id']} → {r['state']}")
```

### 3. 🤖 CI/CD Integration

Integrate into your deployment pipelines to trigger DAG run inspection after a deploy — verify that critical DAGs ran successfully before marking a release as healthy.

```python
import sys
from AirFlowDagFetch import get_dag_runs

runs = get_dag_runs("post_deploy_checks", limit=1)["dag_runs"]
if runs[0]["state"] != "success":
    print("Post-deploy DAG failed!")
    sys.exit(1)
```

### 4. 🔄 Automated Task State Management

Build scripts that automatically scan for `failed` tasks across multiple DAGs and batch-patch them — useful in staging environments where you want to unblock downstream processes quickly.

```python
from AirFlowDagFetch import get_dag_runs
from AirFlowTaskFetch import get_task_instance
from AirFlowTaskChanges import change_task_state

dags_to_check = ["pipeline_a", "pipeline_b", "pipeline_c"]

for dag in dags_to_check:
    runs = get_dag_runs(dag, limit=1)["dag_runs"]
    if runs and runs[0]["state"] == "failed":
        print(f"Patching failed tasks in {dag}...")
        # Fetch and patch specific tasks as needed
```

### 5. 🔍 Task Debugging & Introspection

Inspect operator type, map index, and execution state for any task without navigating the Airflow UI — ideal when working over SSH on a remote server.

```python
from AirFlowTaskFetch import get_task_instance

details = get_task_instance("my_dag", "run_id_here", "transform_step")
print("Operator:", details["operator"])
print("State:", details["state"])
print("Start Date:", details.get("start_date"))
print("End Date:", details.get("end_date"))
```

### 6. 📈 Monitoring & Alerting Integration

Embed this toolkit in your monitoring setup to send Slack/email alerts when DAGs fail, polling the Airflow API on a schedule.

```python
import time
from AirFlowDagFetch import get_dag_runs

def poll_dag(dag_name, interval=60):
    while True:
        runs = get_dag_runs(dag_name, limit=1)["dag_runs"]
        if runs and runs[0]["state"] == "failed":
            # send_slack_alert(f"DAG {dag_name} failed!")
            print(f"ALERT: {dag_name} failed at {runs[0]['run_after']}")
        time.sleep(interval)
```

### 7. 🏗️ Extending to Other Airflow Environments

All functions accept a `host` parameter, making it trivial to target multiple Airflow deployments (dev, staging, prod).

```python
from AirFlowDagFetch import get_dag_runs

envs = {
    "dev":  "http://dev-airflow.internal:8080",
    "prod": "http://prod-airflow.internal:8080",
}

for env, host in envs.items():
    runs = get_dag_runs("critical_etl", host=host, limit=1)["dag_runs"]
    print(f"[{env}] Latest state: {runs[0]['state']}")
```

---

## ⚙️ Configuration

| Variable | Where | Description |
|----------|-------|-------------|
| `host` | Each function parameter | Airflow base URL (include `http://`) |
| `auth` | `AirFlowDagFetch.py` | Basic auth credentials |
| `limit` | `get_dag_runs()` | Max DAG runs to return |
| `order_by` | `get_dag_runs()` | Sort order for DAG runs |

> ⚠️ **Security Note:** Never hardcode credentials in production. Use environment variables, a `.env` file with `python-dotenv`, or a secrets manager.

---

## 🐳 Docker Support

If you're running Airflow via Docker Compose, make sure port `8080` is exposed:

```yaml
# docker-compose.yml excerpt
services:
  airflow-webserver:
    ports:
      - "8080:8080"
```

Then set `host="http://localhost:8080"` (or the container's network address) in your function calls.

---

## 🚧 Roadmap

Upcoming features and improvements, organized by category. Contributions welcome!

### 🔐 Authentication & Security

- [ ] **`python-dotenv` integration** — load `AIRFLOW_USER`, `AIRFLOW_PASSWORD`, and `AIRFLOW_HOST` from a `.env` file automatically
- [ ] **Bearer token / JWT support** — authenticate via Airflow API tokens instead of basic auth (required for some enterprise setups)
- [ ] **Per-environment credential profiles** — store named credential sets (dev/staging/prod) in a local config file (`~/.airflow-manager/config.yaml`)
- [ ] **Kerberos & LDAP auth support** — for organizations using enterprise Airflow deployments

---

### 🖥️ CLI & Developer Experience

- [ ] **`argparse` / `click` CLI interface** — replace interactive prompts with proper command-line flags:
  ```bash
  python main.py --dag my_pipeline --run latest --task extract_data --patch-state success
  ```
- [ ] **`--dry-run` flag** — preview exactly which task state changes would be applied, without making any API calls
- [ ] **`--json` output flag** — print raw API responses as formatted JSON for scripting and piping
- [ ] **`--quiet` / `--verbose` flags** — control log output level
- [ ] **Shell auto-completion** — tab-complete DAG names and task IDs using cached metadata
- [ ] **Color-coded terminal output** — highlight `failed` in red, `success` in green, `running` in yellow using `colorama` or `rich`
- [ ] **`rich` table display** — render DAG runs and task details as formatted terminal tables instead of plain text

---

### 🔄 State Management

- [ ] **Patch to any state** — support `failed`, `skipped`, `queued`, `up_for_retry`, and `cleared` in addition to `success`
- [ ] **Batch task patching** — patch multiple task IDs in a single command across one or many DAG runs
- [ ] **Cascade patching** — patch a task and automatically propagate state changes to all upstream or downstream dependencies
- [ ] **Clear task instances** — add a `clear_task()` function to reset tasks back to `None` state, triggering a re-run
- [ ] **Trigger DAG runs** — POST a new manual DAG run directly from the CLI with optional `conf` JSON payload
- [ ] **Pause / unpause DAGs** — toggle a DAG's active scheduling state without opening the Airflow UI

---

### 📊 Observability & Reporting

- [ ] **JSON / CSV export** — export DAG run history and task summaries to structured files for downstream analysis
- [ ] **Task duration statistics** — calculate average, min, and max execution time per task across recent runs
- [ ] **Failure rate report** — show which tasks fail most frequently across all DAG runs over a time window
- [ ] **DAG health summary** — a single-command overview showing the latest run state for every DAG in your Airflow instance
- [ ] **Run timeline visualization** — ASCII or HTML timeline of task execution order and duration within a DAG run
- [ ] **Log fetching** — retrieve and display task logs directly in the terminal via `GET /api/v2/.../logs`

---

### 🔗 Integrations

- [ ] **Slack notifications** — send alerts to a Slack webhook when a DAG run fails or a patch is applied
- [ ] **Email alerts** — SMTP-based email notifications for failure events or batch patch results
- [ ] **PostgreSQL / SQLite logging** — persist DAG run fetch history and patch audit logs to a local database
- [ ] **Prometheus metrics endpoint** — expose DAG run success/failure counts as Prometheus-compatible metrics
- [ ] **GitHub Actions workflow** — ready-to-use `.github/workflows/check-dags.yml` that runs post-deploy DAG health checks in CI
- [ ] **Apache Kafka event streaming** — publish DAG state change events to a Kafka topic for real-time downstream consumers

---

### 🧪 Testing & Code Quality

- [ ] **Unit tests with `pytest`** — mock Airflow API responses and test all fetch/patch functions in isolation
- [ ] **Integration test suite** — spin up a local Airflow instance via Docker and run end-to-end API tests
- [ ] **Type annotations** — add full `typing` hints to all function signatures for better IDE support
- [ ] **`pyproject.toml` packaging** — package as an installable Python module (`pip install airflow-dag-manager`)
- [ ] **Pre-commit hooks** — `black`, `ruff`, and `mypy` checks enforced on every commit
- [ ] **GitHub Actions CI** — automated linting and test runs on every pull request

---

### 🌐 Multi-Environment & Scalability

- [ ] **Named environment profiles** — switch between `dev`, `staging`, and `prod` configs with a single flag: `--env prod`
- [ ] **Async API calls** — use `asyncio` + `aiohttp` to fetch multiple DAGs or tasks concurrently for faster bulk operations
- [ ] **Rate limiting & retry logic** — automatic exponential backoff on `429 Too Many Requests` and transient network errors
- [ ] **Pagination support** — auto-paginate through all DAG runs when `total_entries` exceeds `limit`, without manual offset handling
- [ ] **Airflow 2.x and 3.x compatibility** — detect API version at runtime and adapt requests accordingly

---

## 🤝 Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you'd like to change.

1. Fork the repository
2. Create your feature branch: `git checkout -b feature/my-feature`
3. Commit your changes: `git commit -m 'Add some feature'`
4. Push to the branch: `git push origin feature/my-feature`
5. Open a Pull Request

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

<p align="center">
  Made with ☕ by <a href="https://github.com/sharvinvarghese">Sharvin Varghese</a>
</p>
