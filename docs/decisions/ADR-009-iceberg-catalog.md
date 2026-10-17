# ADR-009 — Iceberg catalog: HadoopCatalog over a shared volume

- **Status:** Accepted
- **Date:** 2026-10-17

## Context

The SRS calls for an Iceberg catalog as a distinct service (SRS 2.1 item 8,
10.1 `iceberg-catalog`). The conventional choice for a local POC is the Iceberg
REST catalog, which both Beam's `IcebergIO` and Trino's Iceberg connector
support as a first-class integration.

There is no published container image for it. Verified against Docker Hub:

- `apache/iceberg-rest` — does not exist.
- `apache/iceberg-rest-catalog` — does not exist.
- `tabularium/iceberg-rest-catalog` — does not exist.

Running a REST catalog would therefore mean building or third-party-pulling an
image, which adds an unpinnable dependency to a project whose central claim is
reproducibility.

## Decision

Use Iceberg's **HadoopCatalog** with the warehouse root at `/warehouse` on the
shared `iceberg_warehouse` volume. HadoopCatalog keeps catalog metadata (table
pointers, schemas, snapshots) inside the warehouse directory itself, so there is
no separate service to run, pin or secure.

Beam's own `IcebergIO` documentation presents HadoopCatalog as the recommended
path for local development.

Trino reaches the same catalog through its `hadoop` catalog type in
`infra/trino/etc/catalog/iceberg.properties`.

## Consequences

- Catalog state is not independently queryable over JDBC. Reconciliation and
  snapshot inspection therefore go through Trino's Iceberg connector and the
  Iceberg Java API, both of which are already part of the design.
- `iceberg.hadoop-catalog.warehouse` must point at the same absolute path in
  every component. `Config::WAREHOUSE_PATH` documents this invariant.
- If a REST or JDBC catalog is ever required, only the catalog implementation and
  its connection properties change; table layouts and pipeline code do not.