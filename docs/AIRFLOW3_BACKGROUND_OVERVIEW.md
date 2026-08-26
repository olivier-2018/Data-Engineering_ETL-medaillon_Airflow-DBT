# Airflow 3 High-Level overview

Airflow 3 is a big step forward in the evolution of Apache Airflow: faster, more modular, more secure, and much easier to operate at scale. Below is a **high‑level overview**, followed by a **drilled‑down tutorial‑style explanation** of the core components you’ll actually use as a data engineer.

I’ll keep the structure clean and senior‑level, with Guided Links for deeper exploration.


## ⭐ High‑Level Overview of Airflow 3  
Airflow 3 is a **workflow orchestration platform** for building, scheduling, and monitoring data pipelines.  
It uses **Python DAGs** to define tasks and dependencies, and it executes them using a pluggable execution engine.

### What Airflow 3 improves:
- **TaskFlow API v2** → cleaner, function‑based DAGs  
- **AIP‑52: Airflow Providers 2.0** → modular, versioned integrations  
- **Better performance** → fewer scheduler bottlenecks  
- **Modern UI** → faster, more responsive  
- **New trigger rules & deferrable operators** → async execution  
- **Better Kubernetes support** → improved K8s executor & Helm charts  
- **Enhanced security** → secrets backend improvements, RBAC refinements  

Airflow 3 is still Airflow — but more stable, more cloud‑ready, and more Pythonic.


## 🧭 Airflow Architecture (Quick Mental Model)
Airflow has four main components:

- **Scheduler** → decides what tasks should run  
- **Executor** → runs tasks (Local, Celery, Kubernetes, etc.)  
- **Workers** → execute the actual Python code  
- **Metadata DB** → stores DAGs, runs, logs, states  
- **Webserver** → UI for monitoring  

Airflow 3 improves the scheduler and executor performance significantly.


## 🔧 Now Let’s Drill Down — Airflow 3 Tutorial

Below is a practical, senior‑level walkthrough of how Airflow 3 is used in real projects.


## 1. Install Airflow 3  
Airflow uses constraints files to ensure dependency compatibility.

```bash
pip install "apache-airflow==3.0.0" --constraint https://.../constraints-3.0.0.txt
```

Or use the official Helm chart for Kubernetes deployments.


## 2. Create Your First DAG (TaskFlow API v2)

Airflow 3 encourages the **TaskFlow API**, which makes DAGs feel like normal Python functions.

```python
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="example_airflow3",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
):

    @task
    def extract():
        return {"value": 42}

    @task
    def transform(data):
        data["value"] *= 2
        return data

    @task
    def load(data):
        print(data)

    load(transform(extract()))
```

This is the modern Airflow style — no more boilerplate operators.


## 3. Understanding Airflow Operators  
Operators define *what* a task does.

Airflow 3 includes:

- **PythonOperator** → run Python code  
- **BashOperator** → run shell commands  
- **HttpOperator** → call APIs  
- **KubernetesPodOperator** → run tasks in K8s  
- **Deferrable Operators** → async tasks that don’t block workers  
- **Sensors** → wait for external conditions (files, tables, events)  

Airflow 3 improves sensors with **deferrable mode**, reducing worker load.

Example:
```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

# Runs a shell command
bash_task = BashOperator(
    task_id="say_hello",
    bash_command="echo 'hello from airflow'",
)

# Runs arbitrary Python
def my_logic(**context):
    print("doing work")

python_task = PythonOperator(
    task_id="run_python",
    python_callable=my_logic,
)

# Submits a Spark job
spark_task = SparkSubmitOperator(
    task_id="run_spark_job",
    application="/opt/jobs/my_job.py",
    conn_id="spark_default",
)

# Runs a SQL assertion, fails the task if it's false
sql_check = SQLCheckOperator(
    task_id="check_row_count",
    conn_id="postgres_default",
    sql="SELECT COUNT(*) > 0 FROM my_table",
)

bash_task >> python_task >> spark_task >> sql_check
```

**Notes:""
Categories worth knowing: 
- action operators (Bash/Python/SparkSubmit — do something), 
- sensor operators (wait for a condition — a file to appear, 
- an external DAG to finish), and transfer operators (move data between two systems, e.g. S3 → Postgres).   
Every operator becomes a task once instantiated inside a DAG.

## 4. Airflow 3 Scheduling

Scheduling decides when a DAG runs.  Dataset scheduling is powerful for medallion architectures.  

Airflow supports:
- Cron schedules  
- Timedeltas  
- Event‑based scheduling (via sensors)  
- Dataset‑based scheduling (Airflow 2.6+)  

Example:
```python
from datetime import datetime, timedelta
from airflow import DAG

with DAG(
    dag_id="example_dag",
    schedule=timedelta(minutes=5),       # or a cron string, "@daily", or an Asset/Asset-list
    start_date=datetime(2025, 1, 1),
    catchup=False,                        # don't backfill missed runs
    max_active_runs=1,                    # only one run of this DAG in flight at a time
) as dag:
```

## 5. Airflow 3  Trigger Rules

Trigger rules control a task's start condition based on its upstream tasks' outcomes.

Airflow 3 adds more flexible trigger rules:  
- `all_success`  - default
- `all_failed`  
- `one_success`  
- `none_failed` - runs if nothing upstream failed — skips count as OK
- `always` or `all_done` (runs regardless of upstream success/failure, just waits for completion)

Example:  
```python
from airflow.providers.standard.operators.bash import BashOperator

a = BashOperator(task_id="a", bash_command="exit 1")   # fails
b = BashOperator(task_id="b", bash_command="echo b")
c = BashOperator(
    task_id="c",
    bash_command="echo c",
    trigger_rule="none_failed",   # runs even if 'a' was skipped, but not if 'a' failed
)

[a, b] >> c
```

Airflow 3 also lets a DAG's schedule be a list of Assets instead of a time interval — the DAG fires once every listed Asset has been updated since its last run (AND-semantics), not on any individual asset's own update:

```python
from airflow.sdk import Asset

with DAG(schedule=[Asset("silver://orders"), Asset("silver://customers")], ...) as dag:
    ...
```

## 6. Airflow 3 Providers (AIP‑52)  
Providers are integrations (AWS, GCP, Databricks, Snowflake, Postgres, Kafka, etc.).

Airflow 3 introduces:

- **Versioned providers**  
- **Independent release cycles**  
- **Better dependency isolation**  

This makes Airflow much more stable in production.


## 7. Airflow 3 Executors  
The executor determines how and where task instances actually run once the scheduler decides they're ready.

### Most common:
- **LocalExecutor** →  runs tasks as local subprocesses on the same machine as the scheduler. Simple, no extra infrastructure, but capped by that one machine's resources. Good fit for single-node/demo deployments.
- **CeleryExecutor** → distributes tasks across a pool of worker machines via a Celery message broker (Redis/RabbitMQ). Scales horizontally, needs a broker + worker fleet to operate. 
- **KubernetesExecutor** → launches each task as its own Kubernetes pod, on demand. Maximum isolation and elastic scaling, but adds Kubernetes as a hard dependency and has higher per-task startup latency (pod scheduling).
- **SequentialExecutor** → runs one task at a time, no parallelism at all. Only really used with SQLite for a trivial local test, never for real workloads.
- **LocalKubernetesExecutor** → hybrid mode  

Example: 
```bash
# airflow.cfg / env var
AIRFLOW__CORE__EXECUTOR = LocalExecutor
AIRFLOW__CORE__PARALLELISM = 32   # global cap on concurrent task instances
```

## 8. Airflow 3 Monitoring  
The UI shows:

- DAG runs  
- Task runs  
- Logs  
- Gantt charts  
- Dependency graphs  
- Trigger rules  
- Dataset events  

Airflow 3’s UI is faster and more responsive.


## 9. Airflow 3 Best Practices  
Here are the patterns senior engineers use:

- Use **TaskFlow API** instead of classic operators  
- Use **deferrable operators** for long waits  
- Use **KubernetesExecutor** for scalable workloads  
- Store secrets in **AWS/GCP/Vault**  
- Keep DAGs small and modular  
- Avoid heavy Python logic inside tasks  
- Use **XCom** only for small metadata  
- Use **Datasets** for cross‑DAG dependencies  


## 🧭 Summary Table

| Topic | Airflow 2.x | Airflow 3 |
|-------|-------------|-----------|
| TaskFlow API | Supported | Improved |
| Scheduler | Good | Faster, more stable |
| Providers | Monolithic | Versioned, modular |
| Sensors | Blocking | Deferrable (async) |
| UI | Good | Modern, faster |
| Kubernetes | OK | Much improved |
| Security | Good | Better secrets integration |


---
---

# Airflow3 DAG design patterns

Airflow 3 DAG design patterns are all about **making pipelines predictable, maintainable, observable, and scalable**.  
The platform gives you a lot of freedom, but senior engineers rely on a handful of proven patterns that make DAGs easier to operate in production.

Below is a **high‑level overview**, followed by a **deep drill‑down** into the most important Airflow 3 DAG design patterns you’ll actually use.


## ⭐ High‑Level Overview  
Airflow 3 DAG design patterns revolve around five pillars:

1. **TaskFlow DAGs** → function‑based pipelines  
2. **Modular DAGs** → small, composable DAGs  
3. **Dataset‑driven DAGs** → event‑based orchestration  
4. **Deferrable tasks** → async sensors & operators  
5. **Dynamic DAGs** → generate tasks at runtime safely  

These patterns help you avoid the classic Airflow pitfalls: giant DAGs, blocking sensors, messy dependencies, and brittle pipelines.


## 🧭 Airflow 3 DAG Design Patterns (Deep Dive)

### 1️⃣ **TaskFlow DAG Pattern**  
The TaskFlow API is the modern way to write DAGs in Airflow 3.  
It makes DAGs feel like normal Python functions.

### Why it matters  
- Cleaner code  
- Automatic XCom handling  
- Easier testing  
- Less boilerplate  
- Better readability  

### Example  
```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def sales_pipeline():

    @task
    def extract():
        return {"amount": 100}

    @task
    def transform(data):
        data["amount"] *= 2
        return data

    @task
    def load(data):
        print(data)

    load(transform(extract()))

sales_pipeline()
```

### When to use  
Always.  
TaskFlow is the default pattern for Airflow 3.


### 2️⃣ **Modular DAG Pattern**  
Instead of one giant DAG, break pipelines into **small DAGs** that each do one thing well.

### Why it matters  
- Faster debugging  
- Clear ownership  
- Better observability  
- Easier retries  
- Smaller blast radius  

### Structure  
```
dags/
  ingest_orders.py
  clean_orders.py
  aggregate_sales.py
  publish_metrics.py
```

### When to use  
Any pipeline with multiple logical stages.


### 3️⃣ **Dataset‑Driven DAG Pattern**  
Airflow 3 supports **Datasets**, which allow DAGs to trigger other DAGs based on data availability rather than time.

### Why it matters  
- Event‑driven orchestration  
- Perfect for medallion architecture  
- Eliminates fragile cross‑DAG dependencies  

### Example  
Producer DAG:

```python
from airflow.datasets import Dataset

orders_dataset = Dataset("s3://bronze/orders")

@dag(schedule="@daily")
def bronze_orders():
    ...
    write_orders_to_s3()
```

Consumer DAG:

```python
@dag(schedule=[orders_dataset])
def silver_orders():
    ...
```

### When to use  
Whenever DAGs depend on upstream data rather than time.


### 4️⃣ **Deferrable Task Pattern**  
Airflow 3 introduces **deferrable operators**, which allow long‑running waits without blocking workers.

### Why it matters  
- Sensors no longer block worker slots  
- Huge cost savings  
- Better scalability  

### Example  
```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensorAsync

wait_for_file = S3KeySensorAsync(
    task_id="wait_for_file",
    bucket_name="my-bucket",
    bucket_key="incoming/data.json"
)
```

### When to use  
Any sensor or long‑running wait (S3, GCS, Snowflake, Databricks, API polling).


### 5️⃣ **Dynamic Task Mapping Pattern**  
Airflow 3 allows you to generate tasks dynamically at runtime.

### Why it matters  
- Perfect for IoT fleets  
- Perfect for per‑customer pipelines  
- Perfect for per‑file ingestion  

### Example  
```python
@task
def get_devices():
    return ["device_1", "device_2", "device_3"]

@task
def process_device(device_id):
    print(f"Processing {device_id}")

process_device.expand(device_id=get_devices())
```

### When to use  
Any scenario where the number of tasks depends on the data.


### 6️⃣ **Branching Pattern**  
Branching lets you choose different execution paths based on logic.

### Example  
```python
from airflow.operators.branch import BranchPythonOperator

def choose_path():
    return "task_a" if condition else "task_b"
```

### When to use  
Conditional workflows (e.g., skip if no new data).


### 7️⃣ **Fan‑In / Fan‑Out Pattern**  
Classic parallelization pattern:

- Fan‑out → run tasks in parallel  
- Fan‑in → aggregate results  

### Example  
```python
parallel_tasks = process_device.expand(device_id=get_devices())

aggregate_results = aggregate(parallel_tasks)
```

### When to use  
Parallel processing of files, devices, customers, partitions.


### 8️⃣ **Idempotent DAG Pattern**  
Every task should be safe to rerun.

### Techniques  
- Use MERGE instead of INSERT  
- Use checkpoints  
- Use unique keys  
- Use atomic writes  

### When to use  
Always.  
Airflow retries tasks automatically.


### 9️⃣ **Retry‑Friendly Pattern**  
Design tasks so retries don’t break downstream logic.

### Techniques  
- Avoid side effects  
- Avoid partial writes  
- Use transactions  
- Use temporary staging tables  


### 🔥 1️⃣0️⃣ **Medallion DAG Pattern (Airflow + DBT)**  
Airflow orchestrates DBT medallion layers:

```
bronze_ingest_dag → silver_transform_dag → gold_publish_dag
```

Datasets make this clean:

- Bronze DAG publishes `bronze.orders`  
- Silver DAG triggers on that dataset  
- Gold DAG triggers on silver datasets  


## 🧭 Summary Table

| Pattern | Purpose | When to Use |
|--------|---------|-------------|
| **TaskFlow** | Clean, Pythonic DAGs | Always |
| **Modular DAGs** | Small, composable pipelines | Medium/large projects |
| **Datasets** | Event‑driven orchestration | Medallion architecture |
| **Deferrable Tasks** | Async sensors | Long waits |
| **Dynamic Mapping** | Runtime task generation | IoT, per‑file |
| **Branching** | Conditional logic | Skip/choose paths |
| **Fan‑In/Fan‑Out** | Parallelization | Batch processing |
| **Idempotent DAGs** | Safe retries | Always |
| **Retry‑Friendly** | Robust pipelines | Always |


## 🎯 Final Takeaway  
Airflow 3 DAG design patterns make pipelines:

- **cleaner** (TaskFlow)  
- **more scalable** (deferrable tasks)  
- **more modular** (small DAGs)  
- **more event‑driven** (datasets)  
- **more dynamic** (task mapping)  
- **more reliable** (idempotent + retry‑friendly)  

Master these patterns and Airflow becomes predictable, elegant, and production‑ready.

---
---
