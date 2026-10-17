# ADR-008 — Object storage: filesystem warehouse instead of MinIO

- **Status:** Accepted
- **Date:** 2026-10-17
- **Supersedes:** SRS 2.1 item 6 / SRS 10.1 (S3-compatible object storage via MinIO)

## Context

The SRS specifies MinIO as the S3-compatible object store for the lakehouse
warehouse. MinIO is no longer a usable dependency. In October 2025 MinIO Inc.
changed the community edition to a *source-only* distribution and withdrew the
official container images from both Docker Hub and quay.io. The upstream
repository is archived and unmaintained.

Verified from the registries on the target machine:

- `docker.io/minio/minio` returns **401 Unauthorized** for every tag.
- `quay.io/minio/minio` likewise refuses pulls.
- The last published image was `RELEASE.2025-09-07T16-13-09Z`.

Third-party mirrors of the image exist, but depending on an unofficial mirror
for a portfolio project is a supply-chain liability, and compiling MinIO from
source would add a non-reproducible build step to the POC. The ecosystem has
moved to SeaweedFS, but that still means an extra service on a 6 GiB Podman
machine, plus S3 endpoint and credentials threaded through every component.

## Decision

The Iceberg warehouse lives on the **local filesystem** of a dedicated named
volume (`iceberg_warehouse`), mounted at the identical path `/warehouse` in
every container that touches it. Catalog management uses Iceberg's
**HadoopCatalog** (see ADR-009).

The lakehouse properties the SRS depends on are unaffected: Iceberg tables,
time travel, snapshots, commits and replay are all fully supported on the
filesystem. Only the storage *protocol* changes.

## Consequences

- One fewer service, fewer credentials, no S3 endpoint configuration and less
  memory pressure on the development VM.
- The Beam application and the inspection engine address the warehouse by the
  same path, which removes an entire class of path/endpoint mismatch bugs.
- The decision is isolated behind `Config::WAREHOUSE_PATH`; adopting S3 later is
  a matter of changing that value plus the catalog configuration, not of
  rewriting the pipeline.