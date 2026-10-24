# Portable CDC Lakehouse POC Runbook

This runbook exercises the local POC on Windows with Podman and PowerShell.
Run commands from the repository root. The stack stores persistent data in
Podman volumes; `rake infra:down` keeps those volumes.

## 1. Prepare the local configuration

```powershell
Copy-Item .env.example .env
```

Change the local Postgres and MinIO passwords in `.env` before exposing the
stack outside the local machine. `.env` and generated runtime credentials are
ignored by Git.

## 2. Start services and create the lakehouse tables

```powershell
bundle exec rake infra:up
bundle exec rake lakehouse:init
```

`infra:up` builds the pinned MinIO source release, starts Postgres, Kafka,
Debezium Connect, the Iceberg REST fixture and Trino, then waits for the
services and creates the configured MinIO bucket.

## 3. Capture the fixture changes

```powershell
bundle exec rake debezium:register
bundle exec rake debezium:status
bundle exec rake fixtures:scenarios
```

The connector snapshots `public.orders` and then captures the deterministic
updates, deletes and inserts. Kafka topic `pcl.public.orders` is the input to the
Beam graph.

## 4. Run the bounded Beam POC

For a finite fixture run, bound KafkaIO to 20 records:

```powershell
$env:PCL_MAX_RECORDS = '20'
bundle exec rake pipeline:direct
Remove-Item Env:PCL_MAX_RECORDS
```

The task builds `pcl-pipeline.jar` in a Podman volume and runs it with
DirectRunner on the Compose network. It appends normalized valid envelopes to
`raw.order_cdc_events` and rejected envelopes to `ops.cdc_dead_letter`. Kafka
tombstone markers are skipped after their corresponding delete envelopes.

For continuous consumption, leave `PCL_MAX_RECORDS=0` in `.env` and run
`bundle exec rake pipeline:direct`. Stop it with Ctrl+C after the desired capture
window.

## 5. Materialize and reconcile current state

```powershell
bundle exec rake lakehouse:materialize
bundle exec rake lakehouse:describe
bundle exec rake query:file[sql/reconciliation.sql]
```

`lakehouse:materialize` is repeatable. It selects the latest source position per
order and merges it into `silver.orders_current`; older events cannot overwrite
newer state. A delete updates a versioned tombstone. Reconciliation compares
the projection to the append-only raw event history.

## 6. Stop or reset the stack

```powershell
bundle exec rake infra:down
```

This stops containers and preserves database, catalog and warehouse volumes.
To destroy all POC state, use the explicit destructive confirmation documented
by `rake infra:reset`.
