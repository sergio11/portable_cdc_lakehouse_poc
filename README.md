# 🧊 Portable CDC Lakehouse 🚀

### Open-source data engineering experiment with Apache Beam, Apache Spark, Flink and Iceberg

Portable CDC Lakehouse is a local Proof of Concept (POC) for capturing database changes, processing them as a stream, and storing both change history and current state in an open table format.

It uses PostgreSQL, Debezium, Kafka, Scala on the Beam Java SDK, Apache Spark, Apache Flink, Apache Iceberg, Apache Polaris, MinIO, Trino, and Dagster. The same Beam graph runs on DirectRunner and on the classic FlinkRunner with separate JobManager and TaskManager containers. Batch transformation runs on Spark, Dagster runs the incremental graph every five minutes, and Trino is strictly read-only.

## ⚠️ Disclaimer

This repository is an open-source engineering experiment intended for learning and architectural exploration. It is a POC, not a production-ready data platform. It does not provide production availability, security hardening, recovery guarantees, performance SLAs, or end-to-end exactly-once delivery.

## 🎯 Project goal

The goal is to make the path from a relational database change to an auditable lakehouse table visible and reproducible. The POC focuses on the boundaries that are easy to get wrong in streaming systems: CDC envelope normalization, source-position ordering, duplicate delivery, delete tombstones, replay, the interaction between Beam runners and Iceberg storage, and the separation between batch transformation and read-only serving.

## ⚖️ Why this architecture

PostgreSQL answers *what is the state now*. This POC also answers *how did it get here, and can I rebuild it*.

| Question | Querying PostgreSQL directly | This POC |
|---|---|---|
| What is the current state? | Yes, simple and fast | Yes, in `silver` |
| What changed, and when? | No, only the live row | Yes, append-only `raw` with before/after images |
| What happened to deleted rows? | No, they are gone | Yes, versioned tombstones |
| Can I reprocess after a rule or bug fix? | No, the changes are lost | Yes, rebuild `silver` from `raw` |
| Can analytics run without loading the operational database? | No, they compete with the application | Yes, Trino and Iceberg |
| Can several consumers read the same stream? | No, they couple to the database | Yes, one Kafka stream, many readers |
| Can I read the table as of a past snapshot? | No | Yes, Iceberg snapshots |

The point is not to read better: it is to **reconstruct, audit, and analyze without touching the operational system**.

The trade-off is explicit: more moving parts (Kafka, Flink, Spark, Iceberg, Polaris, Trino), seconds of latency, and duplicated storage. For a small dataset that only needs the current row, a direct `SELECT` is the better engineering choice.

## ✨ Key highlights

- 🐘 Capture PostgreSQL changes from the WAL with Debezium.
- 📨 Publish CDC events to Kafka and consume them with a Beam graph.
- 🔀 Run the graph with DirectRunner or submit it to a separate Flink cluster.
- 🧾 Keep append-only CDC history in Iceberg on MinIO.
- 🪦 Project the history into one current-state row per order, preserving deletes as versioned tombstones.
- ⚡ Transform with Spark SQL: Iceberg DDL, an incremental `MERGE`, and replay.
- 🗓️ Run the incremental graph on a Dagster schedule, with asset checks gating each run.
- 🔁 Replay raw history into an isolated Iceberg table.
- 🔎 Serve with Trino, which is configured read-only at the engine level.
- 📦 Run local dependencies with Podman; operate the POC through Rake commands.

## 🏗️ Architecture

```mermaid
flowchart TD
    PG[(PostgreSQL<br/>orders + WAL)] -->|logical decoding| DBZ[Debezium<br/>Kafka Connect]
    DBZ -->|CDC envelopes| K[(Kafka<br/>pcl.public.orders)]
    K --> BEAM[Beam graph<br/>DirectRunner or FlinkRunner]

    BEAM -->|valid events| RAW[(Iceberg raw<br/>order_cdc_events)]
    BEAM -.->|invalid envelopes| DLQ[(Iceberg ops<br/>cdc_dead_letter)]

    RAW --> SPARK[Spark SQL<br/>DDL, MERGE and replay]
    SPARK --> SILVER[(Iceberg silver<br/>orders_current)]
    SPARK --> STATE[(Iceberg ops<br/>materialization_state)]

    RAW --> TRINO[Trino<br/>read-only serving]
    SILVER --> TRINO

    DAG[Dagster<br/>every 5 minutes] -.->|lag and offsets| K
    DAG -.->|bounded read| BEAM
    DAG -.->|MERGE and replay| SPARK
```

Polaris serves the Iceberg catalog and MinIO stores the data files for every table above; Trino also reads `ops.cdc_dead_letter` and `ops.materialization_state`. Replay writes `silver.orders_current_replay_ID`, an isolated table that the diagram omits for clarity.

### 🔄 Runtime flow

1. Dagster's five-minute schedule starts a run. The run compares the topic's end offsets with the consumer group's committed offsets, so it knows exactly how many records are pending before starting anything.
2. PostgreSQL writes changes to its logical WAL. Debezium reads those changes and publishes envelopes to Kafka.
3. Beam reads the topic, ignores Kafka's null-value delete markers, normalizes Debezium envelopes, and routes valid events to raw history and invalid envelopes to the dead-letter table. The run commits the consumer offsets only when the append succeeded and the number of records read matches the lag; a short read leaves the offsets alone, so the next run re-reads and deduplicates by `event_id`.
4. With DirectRunner, the Beam process executes locally in its temporary container. With FlinkRunner, that container submits the job and waits while Flink executes Beam operators in the TaskManager. The streaming job checkpoints every 10 seconds with RocksDB and externalized checkpoints, so a restart has a durable state snapshot. In continuous mode, the submitter and Flink services stay active until the job is stopped.
5. Spark SQL materializes `silver.orders_current`. It reads the watermark from `ops.materialization_state`, selects the keys that received arrivals since the last run, re-ranks the full history of only those keys, applies an idempotent `MERGE`, and advances the watermark after the commit. Deletes become versioned tombstones. Raw rows are deduplicated by `event_id` before ranking, because Kafka delivery is at-least-once. A run with no raw event newer than the watermark skips Spark entirely.
6. `lakehouse:replay` and `dagster:replay` drop and recreate an isolated target table with Spark and rebuild it from all raw events or an inclusive LSN range. They do not update the canonical current-state table or the watermark.
7. Trino only reads: it inspects the Iceberg tables, snapshots, counts, and reconciliation discrepancies. Its access control rejects every write operation.

The scheduled run, from the orchestrator's point of view:

```mermaid
sequenceDiagram
    participant G as Dagster
    participant K as Kafka
    participant B as Beam
    participant R as Iceberg raw
    participant X as Spark
    participant S as Iceberg silver

    G->>K: lag = end offset - committed offset
    G->>B: bounded read of that lag
    K->>B: CDC envelopes
    B->>R: append valid, dead-letter invalid
    G->>K: commit offsets after the append
    G->>X: run when raw is newer than the watermark
    X->>R: read keys changed since the watermark
    X->>S: MERGE and advance the watermark
```

The sequence shows the orchestrator's view of one scheduled run; PostgreSQL and Debezium keep filling Kafka in the background. The unbounded Flink path uses the same Beam graph: the submitter container hands it to the JobManager and TaskManager, which execute it continuously and checkpoint every 10 seconds.

## 🧰 Technologies and why they were chosen

### 🐘 PostgreSQL — operational source

PostgreSQL represents the application database whose changes need to reach the lakehouse. Its logical WAL gives Debezium a change stream to capture, while the sample `orders` table keeps the POC grounded in normal relational data. The current implementation focuses on one PostgreSQL source and the `orders` entity.

### 🔌 Debezium and Kafka Connect — CDC capture

Debezium reads the PostgreSQL WAL and emits structured change envelopes, including an initial snapshot and subsequent creates, updates, and deletes. This avoids implementing a custom database poller and gives the pipeline source metadata it can use for ordering. Kafka Connect hosts and manages the connector.

### 📨 Apache Kafka — decoupled event transport

Kafka buffers the CDC stream between the source connector and Beam. This lets capture and processing run independently and gives consumers a replayable topic. The local broker uses KRaft, so the single-node POC does not need ZooKeeper. This setup demonstrates the integration path; it is not a highly available Kafka deployment.

### 🧠 Scala and Apache Beam — shared processing graph

The pipeline is written in Scala 2.13 using Beam's Java SDK. Beam provides one graph for reading Kafka, normalizing and validating events, routing invalid input, and writing Iceberg rows. Keeping those transforms in a Beam graph makes runner portability demonstrable. Beam does not publish a separate official Scala SDK, so Scala/JVM serialization and classloading are explicit compatibility concerns in this POC.

### 🏃 DirectRunner and FlinkRunner — local and distributed execution

DirectRunner executes the graph locally and is useful during development. The classic FlinkRunner submits the same JVM graph to the local Flink JobManager; Flink schedules and executes the operators in the TaskManager. This makes the persistent distributed runtime visible in Podman while keeping the business transforms shared. A separate Beam worker service is not required for this classic JVM runner. The continuous job runs on the Flink cluster with checkpointing; result-by-result equivalence between the two runners is not claimed.

### ⚡ Apache Spark — batch transformation layer

Spark owns everything that writes `silver`: the Iceberg DDL, the incremental `MERGE` from `raw`, and the replay rebuild. It runs as a one-shot `spark-sql` container on the Compose network, so the POC needs no long-lived cluster; the image is built once with the Iceberg runtime baked in. Keeping transformation out of Beam avoids a stateful streaming job and a custom Iceberg sink, and keeping it out of Trino preserves the layer boundary: Trino serves, Spark transforms. The artifact that is portable here is the SQL, not a runner.

### 🧊 Apache Iceberg — open table format

Iceberg stores both the append-only event history and the current-state projection. The raw table preserves the sequence of accepted changes for inspection and replay. The silver table holds one row per order and records deletes as versioned tombstones so older changes cannot silently restore a deleted order. Beam appends to `raw`, Spark writes `silver`, and Trino reads both. Iceberg snapshots make committed table changes inspectable.

### 🗄️ MinIO — local S3-compatible object storage

MinIO gives Beam, Spark and Trino a shared S3 API endpoint without requiring a cloud account. Iceberg data files live in its persistent Podman volume. The POC builds a pinned upstream MinIO release locally, so the binary and the image are reproducible and the version does not drift.

### 📚 Apache Polaris — shared Iceberg REST catalog

Beam, Spark and Trino resolve the same Iceberg tables and snapshots through one REST catalog endpoint. Apache Polaris provides that endpoint, and its metadata lives in a dedicated `polaris` database inside the same PostgreSQL instance as the source (schema `polaris_schema`), so catalog state survives container restarts and needs no separate store. Clients authenticate with OAuth2 client credentials and use their own S3 credentials for data files, since MinIO does not provide STS.

### 🔎 Trino — read-only serving layer

Trino is the serving layer: it answers business queries, reconciliation, and snapshot inspection. It does not create tables, merge, or replay. The POC mounts Trino's built-in `read-only` system access control, so `INSERT`, `CREATE`, `MERGE`, `DROP`, and `ALTER` are rejected by the engine even if they arrive from the CLI, a notebook, or a BI tool. `rake query:file` adds an allowlist on top of that boundary for a clearer error message.

### 🗓️ Dagster — scheduled orchestration

Dagster is the scheduled entrypoint, not a second implementation: every asset runs the same pinned Spark and Beam containers the Rake tasks run, through the Podman socket. `raw_cdc_events` measures the consumer-group lag with `confluent-kafka`, starts a bounded Beam read of exactly that many records, and commits the offsets only when the append succeeded and the record count matches the lag. A short read leaves the offsets untouched, so the next run re-reads and deduplicates on `event_id`. `orders_current` skips Spark when no raw event arrived after the watermark, so an idle tick costs one query instead of a JVM and an Iceberg overwrite. Two asset checks gate each run: no envelope in the batch was quarantined, and the projection still agrees with the replayed history.

Run and event history live in a `dagster` database inside the same PostgreSQL instance as the source, so the orchestrator survives container restarts and needs no extra service. The webserver runs in Compose; the daemon is started by `rake dagster:up` with the engine client, which mounts the Podman socket reliably on Windows. Replay is deliberately outside the schedule: `rake dagster:replay[id,from_lsn,to_lsn]` runs the replay job with an explicit window.

### 📦 Podman, Compose, and Rake — reproducible local operations

Podman runs the services and job containers without requiring Docker Engine. Compose describes the pinned services, named data volumes, and optional Flink cluster. Rake provides repeatable commands for startup/readiness, builds, pipeline submission, replay, queries, and shutdown. Dagster owns the unattended path; these tasks remain the manual and test path.

### 📌 Pinned versions and validation

| Component | Version | Validation |
|---|---|---|
| Podman / podman-compose | 5.8.2 / 1.5.0 | Local stack started and exercised |
| Build image / JDK | `scala-sbt:eclipse-temurin-21.0.12_8_1.13.0_2.13.18` / Temurin 21.0.12+8 | Build and test gate |
| Scala / sbt | 2.13.18 / 1.13.0 | Compile and tests |
| Apache Beam / DirectRunner | 2.76.0 | Bounded Kafka-to-Iceberg run |
| Beam FlinkRunner / Apache Flink | 2.76.0 (`beam-runners-flink-2.2`) / 2.2.1 | Bounded job completed on Podman JobManager and TaskManager |
| Apache Spark | 3.5.9 (Scala 2.12, Java 17) | Iceberg DDL, incremental MERGE, replay |
| Iceberg Spark runtime / AWS bundle | 1.11.0 | Baked into the Spark image, no runtime downloads |
| PostgreSQL / Kafka | 17.2 / 3.9.1 | Source CDC and topic exercised |
| Debezium Connect | 3.2.4.Final | PostgreSQL connector task reached RUNNING |
| Apache Iceberg | 1.11.0 | Beam write, Spark transform, Trino read |
| Apache Polaris | 1.8.0 | OAuth2 catalog API, Postgres metastore, MinIO storage |
| Trino | 476 | Read-only queries, reconciliation, snapshot inspection |
| Dagster / dagster-postgres | 1.13.27 / 0.29.27 (Python 3.12, SQLAlchemy 2.0.55) | Scheduled runs, asset checks, Postgres-backed history |
| confluent-kafka (Dagster image) | 2.16.0 | Consumer-group lag measurement and offset commit |
| MinIO | `RELEASE.2025-10-15T17-29-55Z`, built with Go 1.24.8 | Bucket and Iceberg object writes exercised |
| AWS CLI | 2.27.41 | MinIO bucket bootstrap |

The FlinkRunner artifact family matches Flink 2.2.x, and [Beam's compatibility table](https://beam.apache.org/documentation/runners/flink/) lists support for Beam 2.75.0 and later with that runner family. The compatibility evidence above is specific to this local pinned stack; it does not claim production-cluster validation.

## 🗂️ Data model and semantics

| Table | Purpose | Important behavior |
|---|---|---|
| `raw.order_cdc_events` | Canonical append-only history | Stores source, key, operation, source position, before/after images, snapshot flag, `source_timestamp`, `connector_timestamp`, `raw_reference` (`topic:partition:offset`), batch ID, and event ID. |
| `silver.orders_current` | Current state for each order | One row per order; latest source position wins; deletes remain as `is_deleted=true` tombstones; `last_operation` and `last_source_timestamp` describe the winning event. |
| `ops.cdc_dead_letter` | Invalid input inspection | Stores the reason code and payload for invalid Debezium envelopes. |
| `ops.materialization_state` | Incremental watermark | One row per target table; `last_ingested_at` is the arrival bound already folded into the projection. |
| `silver.orders_current_replay_<id>` | Isolated replay result | A run-scoped target created by `rake lakehouse:replay`; the canonical projection is left untouched. |

Ordering uses LSN, then the numeric transaction id, then the intra-transaction `sequence` ordinal, then the deterministic event id. Event time and arrival time are not used as the sole precedence rule. Snapshot rows are marked distinctly; deterministic event IDs include the primary key because PostgreSQL snapshot rows can share a source position.

`raw.order_cdc_events` is append-only by design. At-least-once Kafka delivery or a restart without committed offsets can append the same `event_id` more than once; materialization and reconciliation therefore deduplicate by `event_id` before ranking, keeping the newest `ingested_at` copy. The duplicate-delivery report in `sql/trino/reconciliation.sql` makes that visible instead of hiding it.

`lakehouse:materialize` is incremental. It uses `ingested_at` (arrival) to find the keys that changed since the last successful run, then re-ranks the full history of only those keys. Source order still decides the winner, so a late event cannot overwrite a newer state. The watermark advances after the `MERGE` commits; a failed run is reprocessed, and the `MERGE` is idempotent. A replay is a full rebuild by definition and never touches the watermark.

## 🚀 Quick start

From Windows PowerShell, copy `.env.example` to `.env`, set the local PostgreSQL, MinIO, and Polaris passwords, and point `PCL_HOST_WORKSPACE` and `PCL_PODMAN_SOCKET` at this machine (Dagster starts Spark and Beam through the host engine). Start the services and prepare the CDC source:

```powershell
Copy-Item .env.example .env
bundle exec rake infra:up          # starts the stack and the Dagster daemon
bundle exec rake lakehouse:init    # Spark creates the raw/silver/ops Iceberg tables
bundle exec rake debezium:register # registers the PostgreSQL connector
bundle exec rake fixtures:scenarios
```

`infra:up` starts PostgreSQL first, ensures the `polaris` and `dagster` databases and `polaris_schema` exist, then starts the rest of the stack and finally the Dagster daemon. The one-shot `polaris-setup` container creates the `pcl` catalog idempotently. Polaris listens on `http://localhost:8181/api/catalog`; its health endpoint is `http://localhost:8182/q/health`. The first `lakehouse:init` builds the pinned Spark image locally; later runs reuse it.

The Dagster schedule is already running and performs the same initialization and incremental steps every five minutes, so the explicit commands above only make the first result immediate. The UI is at [localhost:3000](http://localhost:3000): `rake dagster:run` triggers the same job on demand, and `rake dagster:replay[readme_proof]` runs a replay through Dagster.

Launch the unbounded Beam job. It keeps reading CDC events from Kafka and writing them to Iceberg until you stop the Flink cluster:

```powershell
$env:PCL_MAX_RECORDS = '0'
bundle exec rake pipeline:flink
```

This command stays attached while the job runs. Open the Flink dashboard at [localhost:8081](http://localhost:8081). To stop the job from a second PowerShell window, run `bundle exec rake pipeline:flink:stop`. The continuous job and Dagster share the `pcl-pg-local-1` consumer group, so run one at a time: stop the scheduler with `bundle exec rake dagster:down` before starting the unbounded job.

The `silver.orders_current` projection is refreshed by Dagster every five minutes. `bundle exec rake lakehouse:materialize` runs the same Spark script by hand when you want the update immediately; `bundle exec rake query:file[sql/trino/reconciliation.sql]` then inspects the result.

## 📸 Results

The stack runs end to end: PostgreSQL changes reach Kafka, Beam appends them to Iceberg, Spark maintains the current-state projection, and Trino reads it. Each subsection shows one capability with the output that proves it.

### 1. Apache Polaris owns the catalog

`rake infra:up` starts PostgreSQL first, creates the `polaris` database and schema, bootstraps the `POLARIS` realm, creates the `pcl` catalog, grants the content privilege and catalog setting that drop-with-purge needs, and only then starts Trino. Every step is idempotent:

```text
$ rake infra:up
Created MinIO bucket 'pcl-warehouse'.

All services ready.
  Trino    http://localhost:8080
  Connect  http://localhost:8083
  MinIO    http://localhost:9001
  Polaris  http://localhost:8181/api/catalog
  Polaris health  http://localhost:8182/q/health
  Dagster  http://localhost:3000
```

```text
$ podman logs pcl-polaris-bootstrap
Realm 'POLARIS' successfully bootstrapped.
Bootstrap completed successfully.

$ podman logs pcl-polaris-setup
Requesting a Polaris access token...
Polaris catalog 'pcl' already exists.
Polaris catalog role 'catalog_admin' already holds CATALOG_MANAGE_CONTENT.
Polaris catalog 'pcl' already enables drop-with-purge.
```

The setup is idempotent: a re-run reports that the catalog and its grants already exist.

The realm is stored in PostgreSQL, and the health endpoint reports `UP`:

![Apache Polaris health endpoint](docs/screenshots/01-polaris-health.png)

### 2. Both runners process the same CDC stream

Debezium reports the connector and its single task as `RUNNING`:

```json
{"name":"pcl-postgres-orders","connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}],"type":"source"}
```

The bounded DirectRunner pass read 15 Kafka records, accepted 13, and counted 2 Kafka tombstones (the key-only delete markers Debezium emits, which carry no payload). It wrote one Iceberg data file:

```text
elements_read=15  accepted_events=13  kafka_tombstones=2  dataFilesWritten=1
```

The FlinkRunner pass ran the same graph on the same topic and produced the same 13 accepted events. Because every pass reads from the consumer group's committed offsets, any batch the group has not committed yet is read again and appended again: the audit table holds 41 rows for 15 distinct event ids, which is at-least-once delivery made visible. The projection absorbs it by deduplicating on `event_id`.

The same job also runs continuously on Flink, where it accumulates checkpoints:

![Beam job running on the Flink dashboard](docs/screenshots/03-flink-job-running.png)

85 checkpoints completed and written to the external directory, so a restart resumes from durable state:

![Flink checkpoint history](docs/screenshots/04-flink-checkpoints.png)

### 3. Spark materializes through Polaris

`rake lakehouse:init` creates the namespaces and tables with a one-shot `spark-sql` container, and `rake lakehouse:materialize` applies the incremental `MERGE`. Both talk to the same Polaris REST catalog as Beam and Trino:

```text
$ rake lakehouse:init
$ podman run --rm --network pcl_default ... localhost/pcl-spark:3.5.9-iceberg-1.11.0 \
    /opt/spark/bin/spark-sql --properties-file /opt/pcl/conf/spark-defaults.conf -f /opt/pcl/conf/work/bootstrap_catalog.sql
Spark master: local[*], Application Id: local-1793131197113
Time taken: 5.53 seconds
Time taken: 0.065 seconds
Time taken: 0.078 seconds
Time taken: 0.525 seconds
Time taken: 0.169 seconds
Time taken: 0.102 seconds
Time taken: 0.112 seconds
Iceberg tables ready (spark-sql, catalog pcl).
```

The first materialization reads the watermark (epoch) and scans every key; later runs rescan only the keys with new arrivals. Because the `MERGE` is idempotent, a repeated batch converges on the same result, but it is not free: Spark rewrites the files that contain matched rows and commits an overwrite snapshot even when no business value changes.

```text
$ rake lakehouse:materialize
$ podman run --rm --network pcl_default ... localhost/pcl-spark:3.5.9-iceberg-1.11.0 \
    /opt/spark/bin/spark-sql --properties-file /opt/pcl/conf/spark-defaults.conf -f /opt/pcl/conf/work/materialize_orders.sql
Spark master: local[*], Application Id: local-1793132074739
Time taken: 7.515 seconds
Time taken: 0.265 seconds
Time taken: 0.366 seconds
Time taken: 0.332 seconds
Time taken: 0.613 seconds
changed_keys	7
Time taken: 15.706 seconds, Fetched 1 row(s)
projected_keys	7
Time taken: 6.102 seconds, Fetched 1 row(s)
Time taken: 18.167 seconds
Time taken: 2.344 seconds
Current-state projection refreshed (spark-sql, catalog pcl).
```

```text
$ rake lakehouse:describe

== tables
ops | cdc_dead_letter
ops | materialization_state
raw | order_cdc_events
silver | orders_current
silver | orders_current_replay_readme_proof
silver | orders_current_replay_window_probe

== current-state snapshots
4709231253593627168 | overwrite | 2026-10-27 21:53:30.220 UTC
8028985869781163391 | overwrite | 2026-10-27 21:44:26.723 UTC
4705716679392310919 | overwrite | 2026-10-27 20:15:40.567 UTC
1414936190202702993 | overwrite | 2026-10-27 19:58:06.729 UTC
1580568179560580880 | overwrite | 2026-10-27 19:47:57.356 UTC
319887680504692394 | overwrite | 2026-10-27 19:45:20.538 UTC
6413925175051381393 | append | 2026-10-27 19:36:03.249 UTC

== materialization watermark
silver.orders_current | 2026-10-27 21:50:53.055 UTC | 2026-10-27 21:53:31.171 UTC
```

Trino cannot write, and it is the engine that enforces it:

```text
$ curl -X POST localhost:8080/v1/statement -d 'CREATE TABLE iceberg.silver.readonly_probe (x BIGINT)'
Access Denied: Cannot create table iceberg.silver.readonly_probe

$ rake "query:file[.local/readonly_probe.sql]"
query:file is read-only and cannot run CREATE.
Allowed: SELECT, WITH, SHOW, DESCRIBE, DESC, EXPLAIN, VALUES, TABLE.
Writes belong to Spark: rake lakehouse:init, lakehouse:materialize, lakehouse:replay.
```

Polaris prefixes each table location with a UUID. The raw table's Parquet and metadata files are visible in the MinIO object browser:

![MinIO object browser with raw order_cdc_events](docs/screenshots/05-minio-raw-data.png)

### 4. Reconciliation and replay

Reconciliation deduplicates by `event_id`, reports the repeated delivery explicitly, and still finds zero discrepancies:

```text
-- 1. Live vs deleted split of the projection.
false | 6
true  | 1

-- 2. Duplicate delivery report.
13 | 26

-- 5. Discrepancy report.
1001 | 39108792 | 39108792 | UPDATE | UPDATE | false | false | ok
1002 | 41398768 | 41398768 | UPDATE | UPDATE | false | false | ok
1003 | 31464104 | 31464104 | SNAPSHOT_READ | SNAPSHOT_READ | false | false | ok
1004 | 31464816 | 31464816 | DELETE | DELETE | true | true | ok
1005 | 31464960 | 31464960 | CREATE | CREATE | false | false | ok
1006 | 31465512 | 31465512 | CREATE | CREATE | false | false | ok
1007 | 31465960 | 31465960 | UPDATE | UPDATE | false | false | ok
```

A full replay into an isolated table reproduces the live projection exactly:

```text
$ $env:PCL_REPLAY_ID='readme_proof'; rake lakehouse:replay
Replay readme_proof materialized to pcl.silver.orders_current_replay_readme_proof: 7 keys, 1 tombstones.
Full replay readme_proof matches silver.orders_current exactly.
```

A bounded replay applies the same logic to an inclusive LSN window, so the target holds only the keys that changed inside it:

```text
$ $env:PCL_REPLAY_ID='window_probe'
$ $env:PCL_REPLAY_FROM_LSN='31464100'; $env:PCL_REPLAY_TO_LSN='31465000'
$ rake lakehouse:replay
Replay window_probe materialized to pcl.silver.orders_current_replay_window_probe: 5 keys, 1 tombstones.
```

```text
SELECT order_id, status, is_deleted, last_lsn
  FROM iceberg.silver.orders_current_replay_window_probe ORDER BY order_id;

1001 | PAID | false | 31464144
1002 | PAID | false | 31464704
1003 | NEW  | false | 31464104
1004 |      | true  | 31464816
1005 | NEW  | false | 31464960
```

Orders 1006 and 1007 are absent because their events fall outside the window. A bounded replay is a partial history by construction, so `lakehouse:replay` reports its shape instead of asserting equality with the live projection.

### 5. Invalid envelopes are quarantined, not dropped

Four deliberately invalid envelopes were published to the same Kafka topic: truncated JSON, an unknown `op`, a delete with no row image, and an update with no primary key. `infra/fixtures/invalid_envelopes.jsonl` holds them, so the check is reproducible:

```powershell
Get-Content infra/fixtures/invalid_envelopes.jsonl |
  podman exec -i pcl-kafka /opt/kafka/bin/kafka-console-producer.sh `
    --bootstrap-server localhost:9092 --topic pcl.public.orders
$env:PCL_MAX_RECORDS = '25'; bundle exec rake pipeline:direct
```

The bounded pass read 19 records, accepted 13, and rejected 4:

```text
elements_read=19  accepted_events=13  rejected_events=4  kafka_tombstones=2  dataFilesWritten=1
```

Each rejection keeps a stable reason code, the original payload, and the run that observed it:

```text
-- 7. Dead letter overview, by reason code.
INCONSISTENT_PAYLOAD | 1
MALFORMED_JSON       | 1
MISSING_PRIMARY_KEY  | 1
UNKNOWN_OPERATION    | 1

MALFORMED_JSON       | - | {"op":"c","after":
UNKNOWN_OPERATION    | - | {"op":"x","after":{"order_id":9001,"status":"GHOST"}
INCONSISTENT_PAYLOAD | - | {"op":"d","source":{"db":"pcl","schema":"public","ta
MISSING_PRIMARY_KEY  | - | {"op":"u","after":{"status":"NO_KEY"},"source":{"db"
```

`event_id` is empty on purpose: the envelope never reached identity derivation, so there is no canonical id to attach. The payload and the Kafka reference are what make the record diagnosable, and none of the four reached `raw` or `silver`.

### 6. Dagster runs the pipeline unattended

The schedule is created in `RUNNING` state, so `rake infra:up` starts orchestrating without a manual trigger:

```text
$ podman exec pcl-dagster-daemon dagster schedule list -w /opt/dagster/app/workspace.yaml
Repository __repository__
*************************
Schedule: pcl_incremental_every_five_minutes [RUNNING]
Cron Schedule: */5 * * * *
```

An `UPDATE` reached PostgreSQL with no manual trigger; the next tick measured a lag of one record, read it, committed offset 21, and merged the keys that had arrived since the watermark:

```text
... raw_cdc_events - Reading 1 pending records from pcl.public.orders.
... raw_cdc_events - Committed offsets {0: 21}.
... orders_current_projection_matches_history - Asset check 'projection_matches_history' on 'orders_current' passed. Description: '0 keys disagree with the history.'
... RUN_SUCCESS - Finished execution of run for "pcl_incremental".
```

The metadata the run recorded:

| Asset | Metadata | Value |
|---|---|---|
| `raw_cdc_events` | pending / read / accepted | 1 / 1 / 1 |
| `raw_cdc_events` | rejected / data files written | 0 / 1 |
| `orders_current` | pending raw events | 1 |
| `orders_current` | changed / projected keys | 2 / 2 |
| `orders_current` | watermark | `2026-10-27 21:50:53.054 UTC` |
| `orders_current` | projection keys (live / tombstone) | 7 (6 / 1) |

Two keys were re-ranked, not one: the order that changed and the key sitting exactly on the previous watermark, which the `>=` detection window re-checks on purpose.

The following tick found nothing new and skipped both external jobs:

```text
... raw_cdc_events - No pending records for pcl.public.orders.
... orders_current - No raw events since the watermark; skipping the MERGE.
... RUN_SUCCESS - Finished execution of run for "pcl_incremental".
```

![Dagster run launched by the schedule, with four assets and two checks](docs/screenshots/02-dagster-runs.png)

Replay is not scheduled. `rake dagster:replay` runs the same Spark script through the same container, with an optional inclusive LSN window:

```text
$ rake "dagster:replay[readme_proof]"
... orders_current_replay - Full replay readme_proof matches silver.orders_current exactly.
... RUN_SUCCESS - Finished execution of run for "pcl_replay".
```

### 7. Terminal evidence

A single capture of the commands behind the state above, from `infra:up` to the read-only rejection:

![Terminal evidence for the full run](docs/screenshots/06-terminal-evidence.png)

### What this shows

- **One source of truth.** Every engine resolves the same Iceberg tables through Polaris, so history, projection and replay cannot drift apart.
- **Portable processing.** The same Beam graph runs locally and on Flink, and the transformation is plain SQL that Spark executes.
- **Auditable by construction.** Raw history is append-only, deletes stay as tombstones, and reconciliation plus a full replay must agree with the live projection.
- **Unattended and guarded.** Dagster runs the graph on a schedule, commits offsets only after a complete read, and a failed check stops the run.

This is a **proof of concept**, not a production-ready platform. Restart/recovery behavior is exercised at the checkpoint-configuration level, not through a sustained failure-injection campaign. The scheduler is a single cron job with two asset checks: it has no retry policy, backfill UI, or alerting, and its daemon is one process, so scheduled runs have a single point of failure. Performance benchmarking, schema evolution, and a second source table remain unvalidated. Exactly-once processing and production throughput are not claimed.

## 📜 License

See [LICENSE](LICENSE).
