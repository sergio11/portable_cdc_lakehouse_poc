# 🧊 Portable CDC Lakehouse 🚀

### Open-source data engineering experiment with Apache Beam, Flink and Iceberg

Portable CDC Lakehouse is a local Proof of Concept (POC) for capturing database changes, processing them as a stream, and storing both change history and current state in an open table format.

It uses PostgreSQL, Debezium, Kafka, Scala on the Beam Java SDK, Apache Flink, Apache Iceberg, Apache Polaris, MinIO, and Trino. The same Beam graph has been exercised with DirectRunner and the classic FlinkRunner on separate JobManager and TaskManager containers.

## ⚠️ Disclaimer

This repository is an open-source engineering experiment intended for learning and architectural exploration. It is a POC, not a production-ready data platform. It does not provide production availability, security hardening, recovery guarantees, performance SLAs, or end-to-end exactly-once delivery.

## 🎯 Project goal

The goal is to make the path from a relational database change to an auditable lakehouse table visible and reproducible. The POC focuses on the boundaries that are easy to get wrong in streaming systems: CDC envelope normalization, source-position ordering, duplicate delivery, delete tombstones, replay, and the interaction between Beam runners and Iceberg storage.

## ✨ Key highlights

- 🐘 Capture PostgreSQL changes from the WAL with Debezium.
- 📨 Publish CDC events to Kafka and consume them with a Beam graph.
- 🔀 Run the graph with DirectRunner or submit it to a separate Flink cluster.
- 🧾 Keep append-only CDC history in Iceberg on MinIO.
- 🪦 Materialize one current-state row per order, preserving deletes as versioned tombstones.
- 🔁 Replay raw history into an isolated Iceberg table.
- 🔎 Inspect tables, snapshots, and discrepancies with Trino.
- 📦 Run local dependencies with Podman; operate the POC through Rake commands.

## 🏗️ Architecture

```mermaid
flowchart LR
    PG[(PostgreSQL<br/>orders + WAL)] -->|logical decoding| DBZ[Debezium<br/>Kafka Connect]
    DBZ -->|CDC envelopes| K[(Kafka<br/>pcl.public.orders)]

    K --> DIRECT[Beam graph<br/>DirectRunner]
    K --> SUBMIT[Beam job submitter<br/>short-lived container]
    SUBMIT --> JM[Flink JobManager]
    JM --> TM[Flink TaskManager<br/>Beam operators execute here]

    DIRECT -->|accepted events| RAW[(Iceberg raw<br/>order_cdc_events)]
    TM -->|accepted events| RAW
    DIRECT -->|invalid envelopes| DLQ[(Iceberg ops<br/>cdc_dead_letter)]
    TM -->|invalid envelopes| DLQ

    RAW --> MAT[Trino materializer<br/>MERGE by source position]
    MAT --> SILVER[(Iceberg silver<br/>orders_current)]
    RAW --> REPLAY[Isolated replay]
    REPLAY --> REPLAY_TABLE[(silver.orders_current_replay_ID)]

    TRINO[Trino SQL] --> RAW
    TRINO --> DLQ
    TRINO --> SILVER
    TRINO --> REPLAY_TABLE

    POL[Apache Polaris<br/>Iceberg REST catalog<br/>Postgres metastore] --- RAW
    POL --- DLQ
    POL --- SILVER
    POL --- REPLAY_TABLE
    MINIO[(MinIO<br/>S3-compatible object store)] --- RAW
    MINIO --- DLQ
    MINIO --- SILVER
    MINIO --- REPLAY_TABLE
```

### 🔄 Runtime flow

1. PostgreSQL writes changes to its logical WAL. Debezium reads those changes and publishes envelopes to Kafka.
2. Beam reads the topic, ignores Kafka's null-value delete markers, normalizes Debezium envelopes, and routes valid events to raw history and invalid envelopes to the dead-letter table.
3. With DirectRunner, the Beam process executes locally in its temporary container. With FlinkRunner, that container submits the job and waits while Flink executes Beam operators in the TaskManager. The streaming job checkpoints every 10 seconds with RocksDB and externalized checkpoints, so a restart has a durable state snapshot. In continuous mode, the submitter and Flink services stay active until the job is stopped.
4. Trino runs an idempotent `MERGE` from raw history into `silver.orders_current`, keeping the highest source position per order. Deletes become versioned tombstones. Raw rows are deduplicated by `event_id` before ranking, because Kafka delivery is at-least-once.
5. `lakehouse:replay` creates an isolated target table and rebuilds it from all raw events or an inclusive LSN range. It does not update the canonical current-state table.
6. Trino queries inspect the Iceberg tables, snapshots, counts, and reconciliation discrepancies.

```mermaid
sequenceDiagram
    participant P as PostgreSQL
    participant D as Debezium
    participant K as Kafka
    participant B as Beam graph
    participant F as Flink TaskManager
    participant R as Iceberg raw (MinIO)
    participant T as Trino materializer
    participant S as Iceberg silver (MinIO)

    P->>D: WAL changes
    D->>K: CDC envelope
    K->>B: Read topic
    Note over B,F: DirectRunner executes locally, or FlinkRunner submits the same graph to Flink
    B->>R: Append valid canonical event
    B->>R: Route invalid envelope to ops dead-letter
    T->>R: Select latest event per key by source position
    T->>S: Idempotent MERGE / versioned tombstone
```

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

DirectRunner executes the graph locally and is useful during development. The classic FlinkRunner submits the same JVM graph to the local Flink JobManager; Flink schedules and executes the operators in the TaskManager. This makes the persistent distributed runtime visible in Podman while keeping the business transforms shared. A separate Beam worker service is not required for this classic JVM runner. The continuous Flink job has been verified running; result-by-result equivalence between runners remains open.

### 🧊 Apache Iceberg — open table format

Iceberg stores both the append-only event history and the current-state projection. The raw table preserves the sequence of accepted changes for inspection and replay. The silver table holds one row per order and records deletes as versioned tombstones so older changes cannot silently restore a deleted order. Iceberg snapshots make committed table changes inspectable.

### 🗄️ MinIO — local S3-compatible object storage

MinIO gives Beam and Trino a shared S3 API endpoint without requiring a cloud account. Iceberg data files live in its persistent Podman volume. The POC builds a pinned upstream MinIO release locally because the upstream community container image is no longer published; this is documented as an implementation constraint.

### 📚 Apache Polaris — shared Iceberg REST catalog

Beam and Trino need to resolve the same Iceberg tables and snapshots. Apache Polaris provides the REST catalog endpoint, and its metadata lives in a dedicated `polaris` database inside the same PostgreSQL instance as the source (schema `polaris_schema`). That removes the SQLite fixture from the POC and makes catalog state survive container restarts. Polaris authenticates clients with OAuth2 client credentials; both Beam and Trino exchange them for short-lived tokens. MinIO does not implement STS, so the catalog is created with `stsUnavailable=true` and credential vending disabled: each client uses its own S3 credentials for data files.

### 🔎 Trino — SQL materialization and inspection

Trino queries Iceberg tables and runs the `MERGE` that materializes the latest source-positioned event per order. Keeping current-state materialization in a separate SQL step isolates it from runner-specific streaming behavior and provides a simple way to inspect the resulting state, snapshots, and discrepancies.

### 📦 Podman, Compose, and Rake — reproducible local operations

Podman runs the services and job containers without requiring Docker Engine. Compose describes the pinned services, named data volumes, and optional Flink cluster. Rake provides repeatable commands for startup/readiness, builds, pipeline submission, replay, queries, and shutdown. These tasks orchestrate the workflow; they are not additional runtime services.

### 📌 Pinned versions and validation

| Component | Version | Validation |
|---|---|---|
| Podman / podman-compose | 5.8.2 / 1.5.0 | Local stack started and exercised |
| Build image / JDK | `scala-sbt:eclipse-temurin-21.0.12_8_1.13.0_2.13.18` / Temurin 21.0.12+8 | Build and test gate |
| Scala / sbt | 2.13.18 / 1.13.0 | Compile and tests |
| Apache Beam / DirectRunner | 2.76.0 | Bounded Kafka-to-Iceberg run |
| Beam FlinkRunner / Apache Flink | 2.76.0 (`beam-runners-flink-2.2`) / 2.2.1 | Bounded job completed on Podman JobManager and TaskManager |
| PostgreSQL / Kafka | 17.2 / 3.9.1 | Source CDC and topic exercised |
| Debezium Connect | 3.2.4.Final | PostgreSQL connector task reached RUNNING |
| Apache Iceberg | 1.11.0 | Beam write, Trino read, shared REST catalog |
| Apache Polaris | 1.8.0 | OAuth2 catalog API, Postgres metastore, MinIO storage |
| Trino | 476 | Table creation, materialization, and SQL inspection |
| MinIO | `RELEASE.2025-10-15T17-29-55Z`, built with Go 1.24.8 | Bucket and Iceberg object writes exercised |
| AWS CLI | 2.27.41 | MinIO bucket bootstrap |

The FlinkRunner artifact family matches Flink 2.2.x, and [Beam's compatibility table](https://beam.apache.org/documentation/runners/flink/) lists support for Beam 2.75.0 and later with that runner family. The compatibility evidence above is specific to this local pinned stack; it does not claim production-cluster validation.

## 🗂️ Data model and semantics

| Table | Purpose | Important behavior |
|---|---|---|
| `raw.order_cdc_events` | Canonical append-only history | Stores source, key, operation, source position, before/after images, snapshot flag, `source_timestamp`, `connector_timestamp`, `raw_reference` (`topic:partition:offset`), batch ID, and event ID. |
| `silver.orders_current` | Current state for each order | One row per order; latest source position wins; deletes remain as `is_deleted=true` tombstones; `last_operation` and `last_source_timestamp` describe the winning event. |
| `ops.cdc_dead_letter` | Invalid input inspection | Stores the reason code and payload for invalid Debezium envelopes. |
| `silver.orders_current_replay_<id>` | Isolated replay result | A run-scoped target created by `rake lakehouse:replay`; the canonical projection is left untouched. |

Ordering uses LSN, then the numeric transaction id, then the intra-transaction `sequence` ordinal, then the deterministic event id. Event time and arrival time are not used as the sole precedence rule. Snapshot rows are marked distinctly; deterministic event IDs include the primary key because PostgreSQL snapshot rows can share a source position.

`raw.order_cdc_events` is append-only by design. At-least-once Kafka delivery or a restart without committed offsets can append the same `event_id` more than once; materialization and reconciliation therefore deduplicate by `event_id` before ranking, keeping the newest `ingested_at` copy. The duplicate-delivery report in `sql/reconciliation.sql` makes that visible instead of hiding it.

## 🛠️ Correctness fixes applied

The first version of the POC exposed several correctness problems. They were fixed in the code and, where possible, covered by tests:

| Problem | Correction |
|---|---|
| Debezium's PostgreSQL field is `sequence`, not `sequenceNo` | `DebeziumSource` decodes `sequence`, tolerates a stringified array or a real array, and exposes the last numeric element as `sequenceNo`. |
| The top-level `ts_ms` was used as the database change time | `sourceTimestamp` now comes from `source.ts_ms`; the top-level value is stored separately as `connectorTimestamp`. |
| Transaction ids were compared as strings (`"999"` > `"1000"`) | `SourcePosition.comparePosition` parses numeric transaction ids and falls back to string comparison only for non-numeric ids. |
| Primary-key columns were hard-coded to `order_id` | `PCL_PRIMARY_KEY_COLUMNS` configures the key columns passed to the normalizer. |
| `raw_reference` was never populated | The Kafka reader now sets it to `topic:partition:offset` for accepted and rejected records. |
| The Flink job had no checkpointing | The cluster and the Beam submission use 10-second `EXACTLY_ONCE` checkpoints, RocksDB state, and externalized checkpoints. |
| Append-only history could contain duplicate events | Materialization and reconciliation deduplicate by `event_id` before ranking; the duplicate report surfaces repeated delivery. |
| Replay reused an existing table | `lakehouse:replay` drops and recreates its isolated target on every run. |
| Polaris `catalog_admin` does not include data-write, and drop-with-purge is off by default, so only the first replay could drop its target | `polaris-setup` grants `CATALOG_MANAGE_CONTENT` and sets `polaris.config.drop-with-purge.enabled=true` on the catalog, so drops with purge work on every run. |
| Reconciliation only compared key counts | It now compares business columns, tombstone state, operation, and source position, and a full replay must match the live projection exactly. |

## 🚀 Quick start

From Windows PowerShell, copy `.env.example` to `.env` and set the local PostgreSQL, MinIO, and Polaris passwords. Start the services and prepare the CDC source:

```powershell
Copy-Item .env.example .env
bundle exec rake infra:up          # starts Postgres, Kafka, MinIO, Polaris, Connect, Trino
bundle exec rake lakehouse:init    # creates the raw/silver/ops Iceberg namespaces and tables
bundle exec rake debezium:register # registers the PostgreSQL connector
bundle exec rake fixtures:scenarios
```

`infra:up` starts PostgreSQL first, ensures the `polaris` database and `polaris_schema` exist, then starts the rest of the stack. The one-shot `polaris-setup` container creates the `pcl` catalog idempotently. Polaris listens on `http://localhost:8181/api/catalog`; its health endpoint is `http://localhost:8182/q/health`.

Launch the unbounded Beam job. It keeps reading CDC events from Kafka and writing them to Iceberg until you stop the Flink cluster:

```powershell
$env:PCL_MAX_RECORDS = '0'
bundle exec rake pipeline:flink
```

This command stays attached while the job runs. Open the Flink dashboard at [localhost:8081](http://localhost:8081). To stop the job from a second PowerShell window, run `bundle exec rake pipeline:flink:stop`.

The `silver.orders_current` projection is refreshed separately; in a second window, run `bundle exec rake lakehouse:materialize` when you want to update it, then inspect it with `bundle exec rake query:file[sql/reconciliation.sql]`.

## 📸 Results from the running POC

The screenshots and outputs below come from one continuous session on 26 October 2026. The same Beam graph ran first with DirectRunner and then as an unbounded streaming job on a separate Flink cluster, both writing to the same Iceberg tables through Apache Polaris.

### 1. Apache Polaris owns the catalog

`rake infra:up` starts PostgreSQL first, creates the `polaris` database and schema, bootstraps the `POLARIS` realm, creates the `pcl` catalog, grants the content privilege that Trino needs to drop tables with purge, and only then starts Trino. Every step is idempotent:

```text
$ rake infra:up
Created MinIO bucket 'pcl-warehouse'.

All services ready.
  Trino    http://localhost:8080
  Connect  http://localhost:8083
  MinIO    http://localhost:9001
  Polaris  http://localhost:8181/api/catalog
  Polaris health  http://localhost:8182/q/health
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

On a fresh volume the same setup container prints `Created Polaris catalog 'pcl' at s3://pcl-warehouse/warehouse/.`; the lines above are the idempotent re-run.

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

The FlinkRunner job started from the earliest offset because no consumer offsets were committed, so it appended a second batch of 13 accepted events. The audit table now holds 26 rows for 13 distinct event ids, which is the at-least-once behavior the projection is designed to absorb.

The job ran with five operators on the local cluster and 10-second `EXACTLY_ONCE` checkpoints:

![Beam job running on the Flink dashboard](docs/screenshots/03-flink-job-running.png)

85 checkpoints completed; the latest one acknowledged 5/5 subtasks at 421 KB and was written to the external checkpoint directory. The six failures in the history are the first triggers while the job was still deploying (`Not all required tasks are currently running`), not state failures:

![Flink checkpoint history](docs/screenshots/04-flink-checkpoints.png)

### 3. Trino materializes through Polaris

`rake lakehouse:materialize` merges the latest source-positioned event per order. The query detail confirms the MERGE ran through the Polaris REST catalog against `iceberg.silver.orders_current`:

![Trino MERGE query detail](docs/screenshots/02-trino-merge-query.png)

Polaris prefixes each table location with a UUID. The raw table's Parquet and metadata files are visible in the MinIO object browser:

![MinIO object browser with raw order_cdc_events](docs/screenshots/05-minio-raw-data.png)

### 4. Reconciliation and replay

Reconciliation deduplicates by `event_id`, reports the repeated delivery explicitly, and still finds zero discrepancies:

```text
-- 1. Live vs deleted split of the projection.
false | 6
true  | 1

-- 2. Duplicate delivery report.
13 | 13

-- 5. Discrepancy report.
1001 | 31461760 | 31461760 | UPDATE | UPDATE | false | false | ok
1002 | 31462320 | 31462320 | UPDATE | UPDATE | false | false | ok
1003 | 31461632 | 31461632 | SNAPSHOT_READ | SNAPSHOT_READ | false | false | ok
1004 | 31462432 | 31462432 | DELETE | DELETE | true | true | ok
1005 | 31462576 | 31462576 | CREATE | CREATE | false | false | ok
1006 | 31463104 | 31463104 | CREATE | CREATE | false | false | ok
1007 | 31463552 | 31463552 | UPDATE | UPDATE | false | false | ok
```

A full replay into an isolated table reproduces the live projection exactly:

```text
$ $env:PCL_REPLAY_ID='readme_proof'; rake lakehouse:replay
Replay readme_proof materialized to iceberg.silver.orders_current_replay_readme_proof: 7 keys, 1 tombstones.
Full replay readme_proof matches silver.orders_current exactly.
```

### 5. Terminal evidence

The complete command output for the run is collected in a single capture:

![Terminal evidence for the full run](docs/screenshots/06-terminal-evidence.png)

## 🔎 What this evidence proves

- PostgreSQL CDC events travel through Debezium and Kafka into Beam, and the FlinkRunner job runs on the local Flink cluster with checkpointing enabled.
- Trino can materialize and inspect Iceberg current state through Apache Polaris; deletes are represented as tombstones.
- Reconciliation compares business columns, tombstone state, operation, and source position, not only key counts. A full replay must match the live projection exactly.
- Raw history is append-only. Repeated delivery is expected under at-least-once processing and is handled by deduplicating on `event_id`; the duplicate report makes it visible.

This is a **proof of concept**, not a production-ready platform. Restart/recovery behavior is exercised at the checkpoint-configuration level, not through a sustained failure-injection campaign. Invalid-message integration checks and performance benchmarking remain unvalidated. Exactly-once processing and production throughput are not claimed.

## 📜 License

See [LICENSE](LICENSE).
