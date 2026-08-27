# Spark in This Project: How a Job Actually Runs

This doc exists because the mapping from "a config setting" to "how many things actually run at
once, on which machine" is genuinely confusing in Spark — even more so once Docker's own resource
limits get layered on top. Everything below uses this repo's real settings and one real job
(`purchase_orders_to_silver.py`) as the running example, not a generic Spark tutorial.

## 1. The vocabulary, in order

Five words get used loosely in casual Spark talk. Here they are, precisely, smallest to largest:

| Term | What it is | This repo's example |
|---|---|---|
| **Core** (a Spark "core") | A unit of Spark's own internal bookkeeping — "one concurrent task slot." **Not** automatically the same thing as one real CPU core on your laptop (see §3). | — |
| **Task** | The actual unit of work. One task = one partition's worth of data, processed start to finish by one thread. | Writing `silver.purchase_orders_current`'s rows for exactly one partition. |
| **Stage** | A group of tasks that can all run without waiting on each other (no shuffle in between). | The write stage: N tasks, one per partition, no task depends on another. |
| **Job** | Everything triggered by one Spark *action* (`.count()`, `.collect()`, `.foreachPartition()`, etc.) — one job is made of one or more stages. | `.count()` is one job. `.repartition(2).foreachPartition(...)` is a separate, later job. |
| **Application** | One `spark-submit` process, one `SparkSession`, start to finish. Can run *many* jobs in sequence, all sharing the same executors and the same core budget. | One Airflow task run = one `spark-submit` = one application. `purchase_orders_to_silver.py`'s `main()` runs several jobs (the orphan-check `.collect()`, the watermark `.collect()`, the write) all inside this **one** application. |

And the machines:

| Term | What it is | This repo's example |
|---|---|---|
| **Master** | The one process that tracks which Workers exist and how many cores/how much memory each has free. Decides which Worker(s) get to run each application's executors. | `spark-master` container. |
| **Worker** | A daemon running on one machine (one container here), advertising "I have this many cores and this much memory available to hand out." | `spark-worker-1`, `spark-worker-2` — two separate containers. |
| **Executor** | A JVM process, launched by a Worker *for one specific application*, that actually runs that application's tasks. An executor is granted some number of cores (task slots) and some amount of memory when it's created. | Created fresh for each Airflow task run, torn down when the job finishes. |
| **Driver** | The process running your actual Python script (`main()`), coordinating everything, not itself an executor. | Runs *inside* `airflow-scheduler` for every batch job (`deploy_mode="client"`) — see `docs/ARCHITECTURE.md`. |

## 2. This repo's actual cluster, right now

```
┌─────────────────────────────────────────────────────────────────┐
│  spark-master  (container_name: spark-master)                   │
│  Tracks: which workers exist, how many cores/how much RAM        │
│  each currently has free. Decides executor placement.           │
│  Docker limits: cpus=0.5, mem_limit=768m (coordination-only,     │
│  no data ever flows through it)                                 │
└─────────────────────────────────────────────────────────────────┘
              │ registers with                │ registers with
              ▼                                ▼
┌───────────────────────────┐    ┌───────────────────────────┐
│  spark-worker-1            │    │  spark-worker-2            │
│  SPARK_WORKER_CORES=4      │    │  SPARK_WORKER_CORES=4      │
│  SPARK_WORKER_MEMORY=1600m │    │  SPARK_WORKER_MEMORY=1600m │
│  Docker: cpus=2.5,          │    │  Docker: cpus=2.5,          │
│  mem_limit=2000m            │    │  mem_limit=1500m            │
└───────────────────────────┘    └───────────────────────────┘

Meanwhile, every batch job's DRIVER (the actual Python script) runs inside:

┌─────────────────────────────────────────────────────────────────┐
│  airflow-scheduler                                                │
│  Every SparkSubmitOperator task's driver process lives here      │
│  (deploy_mode="client") - it submits work to spark-master,       │
│  which then launches executors on the two workers above.         │
└─────────────────────────────────────────────────────────────────┘
```

`spark-streaming-truck-position` is the one exception — its own dedicated always-on container, driver
and all, capped at `spark.cores.max=1` permanently (see `docs/ARCHITECTURE.md`).

## 3. Two completely separate layers of "how many cores" — the actual source of confusion

This is the single most important thing to internalize, and it's why "give each executor 2 of my
laptop's cores" doesn't map onto one setting.

**Layer 1 — Docker/the OS.** `cpus: 2.5` in `docker-compose.yml` is a real, enforced cgroup quota:
the Linux kernel will not let that container's processes use more than 2.5 CPU-seconds of real time
per wall-clock second, no matter what. This is the actual physical resource.

**Layer 2 — Spark's own internal accounting.** `SPARK_WORKER_CORES=4` is *Spark's Worker daemon*
being told "you may tell the Master you have 4 cores to hand out." This number is **pure bookkeeping
inside Spark** — it has no automatic connection to the container's real Docker CPU quota. Spark will
happily believe it has 4 schedulable slots and hand out executors accordingly, even though the
container is only guaranteed 2.5 real CPU-seconds per second by Docker.

```
┌─────────────────────────────────────────────────────┐
│  spark-worker-1 container                             │
│                                                         │
│  Docker/cgroup layer:  "you get 2.5 real CPU-seconds   │
│                         per second, hard limit"        │
│                                                         │
│  Spark's own layer:    "I (the Worker) will tell the   │
│                         Master I have 4 cores to give  │
│                         out" - SPARK_WORKER_CORES=4    │
│                                                         │
│  These two numbers are set independently and Spark     │
│  never checks the first one - it just schedules more   │
│  concurrent tasks than the container can truly run     │
│  in parallel if you tell it to (they'll queue/         │
│  contend for real CPU time, not crash).                │
└─────────────────────────────────────────────────────┘
```

**Practical consequence, in your own words**: if you want to actually give an executor "2 of my
laptop's cores," you need to set **both** layers consistently — the container's Docker `cpus:`
(the real ceiling) *and* `SPARK_WORKER_CORES`/`spark.cores.max` (what Spark is allowed to
schedule) to matching, sane numbers. Setting only one of them either wastes real capacity Spark
never asks for, or lets Spark oversubscribe a container that can't actually deliver that much
concurrent CPU time.

## 4. Every setting in this repo, what it actually controls

| Setting | Scope | Set where (this repo) | What it controls |
|---|---|---|---|
| `cpus:` / `mem_limit:` | One Docker container | `docker-compose.yml`, per service | **Real, OS-enforced** CPU/memory ceiling for that container. Independent of everything below. |
| `SPARK_WORKER_CORES` | One Worker daemon | `docker-compose.yml` env, `spark-worker-1`/`-2` | How many cores *this Worker* advertises to the Master as available to hand out — Spark's own accounting, not OS-enforced. |
| `SPARK_WORKER_MEMORY` | One Worker daemon | same | Same idea, for memory. |
| `spark.cores.max` | One **application** | Each DAG file's `SPARK_CONF`/`conf={...}` (e.g. `silver_purchase_orders_dag.py:20`) | Total core ceiling for *this one spark-submit run*, across however many executors the Master decides to create for it. **Not per-executor.** |
| `spark.executor.memory` | Per executor | Same `conf={...}` dicts | Memory granted to *each* executor this application gets (there can be several). |
| `spark.driver.memory` | The driver process | Same | Memory for the driver (here: inside `airflow-scheduler`). |
| `spark.deploy.spreadOut` | The whole **cluster** (Master-level) | **Not set anywhere in this repo** — using Spark's own default, `true` | When an application asks for N cores, should the Master spread them across as *many workers* as possible (many thin executors), or pack them onto as *few workers* as possible (few thick executors)? This is the setting that actually decided our two workers each got a 1-core executor instead of one worker getting a 2-core executor. |
| `spark.task.cpus` | Per task | **Not set anywhere** — default `1` | How many cores one task consumes while running. Always 1 here — a task is single-threaded code operating on one partition; it never uses more than 1 core no matter how many cores its executor has (see §6). |
| `spark.scheduler.minRegisteredResourcesRatio` | Per application | **Not set** — default varies (0.8 in Standalone mode) | What fraction of the requested executor resources must have registered with the driver *before scheduling starts at all*. This is the knob that would remove the race described in §5. |

## 5. Walking one real job end to end: `purchase_orders_to_silver.py`

This job's `SPARK_CONF` (silver_purchase_orders_dag.py:20) sets `spark.cores.max=2`. Here's exactly
what happens, in order:

```
1. Airflow's SparkSubmitOperator runs spark-submit inside airflow-scheduler.
   -> One new Spark APPLICATION is born. Driver = the Python process running main().

2. The driver asks spark-master for up to 2 cores total.

3. spark-master looks at its two registered Workers (4 free cores each) and, because
   spreadOut=true (the default, never overridden here), decides to SPREAD the 2-core
   request across BOTH workers rather than packing it into one:

        spark-worker-1: gives 1 core -> one executor, 1 core
        spark-worker-2: gives 1 core -> one executor, 1 core

   (Confirmed live in this project's own logs: every run shows two separate
   "Executor added ... with 1 core(s)" lines, one per worker, for this exact job.)

4. Inside main(), several JOBS run one after another, all sharing these same 2 executors:
     - fetch_incremental() queries (plain psycopg2, no Spark tasks at all)
     - the orphan-check `.collect()` -> its own small job
     - `final.count()` -> another small job
     - `final.repartition(2).foreachPartition(_write_partition)` -> the job that matters here

5. repartition(2) reshapes the DataFrame into exactly 2 partitions (round-robin split -
   see docs/ARCHITECTURE.md's Kafka-partitioning section for why round-robin, not hash,
   was chosen). This means the NEXT action will create exactly 2 TASKS - one per partition.

6. Those 2 tasks get offered to whichever executor(s) are registered and idle AT THAT
   EXACT MOMENT. Since each executor here only has 1 core = 1 task slot, one of two
   things happens:
```

```
  OUTCOME A (observed, in practice, in this project):           OUTCOME B (the "ideal" case):
  both tasks land on the SAME executor                          tasks land on DIFFERENT executors

  spark-worker-1 executor (1 slot)     spark-worker-2 executor    spark-worker-1 executor (1 slot)     spark-worker-2 executor
  ┌─────────────────────────┐         ┌──────────────┐          ┌─────────────────────────┐         ┌─────────────────────────┐
  │ task 0 (partition 0)     │         │   (idle -     │          │ task 0 (partition 0)     │         │ task 1 (partition 1)     │
  │  runs, finishes           │         │  never used) │          │  runs                    │         │  runs                    │
  │ then task 1 (partition 1)│         │              │          │  AT THE SAME TIME as -->  │         │  <-- this one            │
  │  runs after it            │         │              │          └─────────────────────────┘         └─────────────────────────┘
  └─────────────────────────┘         └──────────────┘

  SEQUENTIAL - task 1 waits for                                   PARALLEL - both writes genuinely
  task 0's core to free up, even                                  happen at the same wall-clock time,
  though 2 partitions/2 tasks exist.                              on two separate machines.
```

Both outcomes are *correct* (same balanced row split, same idempotent upsert SQL, same end
result in Postgres) — the difference is only whether the two writes overlap in time. Outcome A is
what a live check of this exact job found (§7 has the fix for forcing Outcome B reliably).

## 5a. What's actually allowed to overlap: tasks vs. stages vs. jobs

Only **tasks** ever get placed onto an executor's core slots — a stage or a job is never itself
"scheduled" anywhere, they're purely driver-side bookkeeping used to sequence tasks correctly. The
three levels behave very differently when it comes to parallelism:

**Tasks within the same stage: yes, genuinely parallel.** This is the actual definition of a stage
— a group of tasks with *no dependency on each other*, since none needs another task's output
first. That's exactly why they're allowed to run at the same time, bounded only by how many free
core slots exist across the executors at that moment. This is the real unit of parallelism Spark
exploits, and it's what §5's Outcome A/B diagram is about.

**Stages within the same job: normally sequential**, because a job gets split into multiple stages
*specifically when* one stage needs the shuffled output of the previous one — a shuffle (data
redistributed across the cluster) is literally the boundary between two stages. Concretely, in this
same job: the `lag()`/`Window.partitionBy("purchase_order_id")` call needs every row for a given
order gathered onto the same partition before it can compute anything, which is a shuffle — so
that `.collect()` job already has (at least) 2 stages: one that shuffles, one that computes the
window function afterward, and stage 2 cannot start until every task in stage 1 has finished.
Same story for `repartition(2)` in the final write: "reshuffle into 2 partitions" and "run
`foreachPartition` on those 2 partitions" are two separate, sequential stages of that one job.

**Jobs within the same application: normally sequential too — but for a different reason.** This
isn't a Spark scheduling rule at all, just a consequence of how `main()` is written: it's an
ordinary top-to-bottom Python script. `main()` calls the orphan-check `.collect()`, waits for that
line to return, *then* calls `final.count()`, waits, *then* calls
`.repartition(2).foreachPartition(...)`. Job 2 is never even submitted to Spark until job 1's
action call has returned control to the Python interpreter. Nothing stops multiple jobs from truly
running concurrently against the same executor pool (Spark's `FAIR` scheduler mode plus a
multi-threaded driver genuinely can interleave them) — nothing in this codebase does that, so every
job here just runs after the previous one because the script calls them one after another.

So, top to bottom for this one job file: **jobs** run in sequence (the script says so); **stages
within a job** run in sequence (shuffle dependencies say so); **tasks within one stage** are the
only thing that can genuinely run at the same wall-clock time — and only if enough free core slots
exist across the executors right when that stage's tasks are offered.

## 6. The one idea that resolves most of the confusion

**A single task never uses more than 1 core, no matter how many cores its executor has.**

Giving an executor 2 cores does not make *one* task (one partition's write) run twice as fast. It
only means that executor can run **2 different tasks at the same time** — e.g. partition 0 and
partition 1's writes, side by side, each still single-threaded, each still using exactly 1 core.

```
Executor with 1 core:          Executor with 2 cores:
┌───────────────┐              ┌───────────────┬───────────────┐
│  1 task slot   │              │  task slot 1   │  task slot 2   │
└───────────────┘              └───────────────┴───────────────┘
2 tasks queue up,               2 tasks run at the same time,
run one after another.          each still using only 1 core each.
```

So "more cores per executor" buys you *more tasks running concurrently on that one executor* — it
never makes an individual task internally faster or more parallel by itself.

## 7. Concrete recipes for what you actually want

**Goal: "split the data in 2, run both halves as real, simultaneous parallel work — not
sequentially."** Three ways to get there, in increasing order of literal cross-machine spread:

**Recipe A — parallel, but both on one worker (simplest, most reliable):**
Give one executor 2 cores instead of splitting 1+1 across two workers. Set
`spark.deploy.spreadOut=false` on `spark-master` (cluster-wide - affects every application, not
just this job) so the Master packs the 2-core request onto a single worker instead of spreading it.
Combined with the existing `repartition(2)`, this reliably yields 2 tasks on 2 slots of the *same*
executor, running genuinely concurrently — no cross-container registration race to worry about.

**Recipe B — parallel, genuinely split across both worker containers (what you originally pictured):**
Keep `spreadOut=true` (already the default — this is what already gives one 1-core executor per
worker). Add `spark.scheduler.minRegisteredResourcesRatio=1.0` (+ a
`spark.scheduler.maxRegisteredResourcesWaitingTime`, e.g. `"10s"`) to the job's `SPARK_CONF`. This
forces the driver to wait until **both** requested executors have actually registered before it
schedules *any* task — removing the timing race that caused Outcome A above. With both 1-core
executors guaranteed ready before scheduling starts, `repartition(2)`'s 2 tasks land one on each.

**Recipe C — "give each worker 2 of my real cores" (matches your original framing exactly):**
Set the Docker-real ceiling and Spark's own accounting *consistently*, per worker:
```yaml
# docker-compose.yml, spark-worker-1 and spark-worker-2:
cpus: '2'                    # real, Docker-enforced ceiling
environment:
  - SPARK_WORKER_CORES=2     # Spark's own accounting matches the real ceiling
```
Then request `spark.cores.max=4` for the job (2 from each worker, since spreadOut=true spreads
across both), and `repartition(4)` so there are exactly 4 tasks — one per real core, two per
worker. This is the version where "2 real cores per worker" and "2 tasks per worker" actually
line up, with no oversubscription in either direction.

## 8. Executor sizing: how many cores per executor, and how many executors?

This is a well-known, genuinely debated tradeoff in Spark, with real failure modes on both extremes
— worth understanding even though it barely matters at this project's data volume (§9 explains why).

**Too many cores per executor (e.g. one 32-core executor):**
- One giant JVM heap → GC pauses get large and unpredictable, and *every* task in that executor
  stalls during a pause, not just one.
- Poor fault isolation: lose that one executor, lose all 32 tasks' in-flight progress at once.
- I/O contention: many threads sharing one JVM's I/O paths can bottleneck rather than scale.

**Too few cores per executor (e.g. 1 core, as in this project's own jobs today):**
- JVM overhead (heap, class loading, broadcast-variable copies) gets paid once *per executor* — many
  1-core executors duplicate that fixed cost far more than necessary.
- More network/scheduling chatter, since broadcast state can't be shared across the JVMs a bigger
  executor would have consolidated.

**The commonly cited sweet spot** (from real production tuning experience, e.g. Cloudera's
well-known Spark tuning guide): **around 4-5 cores per executor** — enough to amortize JVM overhead
and get real I/O throughput, not so many that GC pauses or failure blast-radius become a problem.
Standard practice also never allocates 100% of a node's cores to Spark — leave headroom for the
node's own OS/daemon processes (or, on Kubernetes, the kubelet).

**This demo vs. a real cloud-native production environment:**

| | This local demo | Real prod |
|---|---|---|
| What dominates runtime | JVM/Spark-app boot time (~10-20s per job) - already far bigger than any task's actual work at this data volume. Executor topology barely moves the needle. | Real data volume and I/O - executor sizing genuinely changes wall-clock time and cost. |
| Executor shape | Current 1-core executors are fine - the point is pedagogical, not throughput. Tuning further wouldn't meaningfully speed anything up. | Size for ~4-5 cores/executor, memory sized to fit a node's budget after OS/daemon headroom - determined by benchmarking the actual workload. |
| Fleet size | Fixed, small (2 workers, decided once in `docker-compose.yml`). | Usually **dynamic** - `spark.dynamicAllocation.enabled=true` requests more executors as more tasks become available and releases them when idle, paired with the cluster's own autoscaler (K8s Cluster Autoscaler, YARN, or a managed platform). Tune the *shape* of one executor once; let the *count* scale with load. |
| Fault tolerance | Not a real concern at this scale/duration. | Genuinely matters: many *moderately*-sized executors reduce blast radius vs. a few giant ones - losing 1 of 50 4-core executors loses far less in-flight work than losing 1 of 5 40-core executors. |
| Placement control | Standalone's manual `spreadOut`/`spark.cores.max`, as in §3-4 above. | On Kubernetes, executors are just pods sized via normal K8s resource requests/limits, scheduled by K8s's own scheduler - Standalone's `spreadOut` concept doesn't even apply there. |

**Bottom line**: for this repo, leave executor sizing alone - any win would come from reducing
per-job JVM boot overhead or job count, not from re-shaping cores-per-executor. For a real prod
deployment of similar logic, benchmark the actual workload to find where GC pauses/I/O throughput
start degrading, converge on something in the 4-5-cores-per-executor range, and let dynamic
allocation handle *how many* of those executors exist at any given moment.

## 9. Quick reference

- **Want visible proof of what actually happened on a given run?** Each executor's own stdout/stderr
  live on the worker container's filesystem at `/opt/spark/work/<app-id>/<executor-id>/std{out,err}` —
  *not* in Airflow's task log, which only captures the driver's own output (see `docs/DEVELOPMENT.md`
  if adding new per-partition diagnostics).
- **Want to see 2 executors get created for a job?** Check `docker logs spark-worker-1`/`spark-worker-2`
  for `"Asked to launch executor app-... for <job-name>"` right after triggering the DAG task.
- **Want to know if 2 tasks actually ran on 2 different executors, or 1 sequentially?** Check the core
  count in the `"Executor added"` log lines (driver log, via
  `docker exec airflow-scheduler bash -c "cat <task log> | grep 'core(s)'"`) — 1 core per executor
  means at most 1 task at a time on that executor, regardless of how many executors exist.
