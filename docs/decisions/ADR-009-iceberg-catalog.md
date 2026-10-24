# ADR-009 — Iceberg REST catalog backed by SQLite

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

Trino 476 does not support HadoopCatalog as an Iceberg catalog type. The POC
needs one catalog endpoint that both the Beam JVM application and Trino can
use, while keeping table data and metadata files in MinIO.

## Decision

Use Apache Iceberg's REST catalog fixture image `1.10.1`, pinned to the latest
published versioned tag, with a SQLite `JdbcCatalog` database persisted on the
`iceberg_catalog_data` Podman volume. Configure the catalog's Iceberg AWS
FileIO to write to `s3://pcl-warehouse/warehouse/` in MinIO. Beam and Trino
connect to the same REST endpoint; Trino accesses the same S3 files using its
native S3 filesystem support.

## Consequences

- Catalog metadata is shared through an explicit REST service instead of local
  catalog paths.
- The fixture is a POC catalog service; SQLite persistence and a single node
  are not production availability guarantees.
- The Iceberg Java library in the application is version 1.11.0 while the
  published fixture is 1.10.1. Compatibility must be verified by creating and
  querying tables through both clients before it is marked verified.
- Bucket, endpoint, region and credentials are configurable outside business
  logic; local secrets are written only to `.env` and the ignored Trino file.
