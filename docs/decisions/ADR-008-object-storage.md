# ADR-008 — Object storage: MinIO

- **Status:** Accepted
- **Date:** 2026-10-24
- **Supersedes:** the local-filesystem deviation recorded in the previous ADR-008

## Context

The SRS requires an S3-compatible object store. The POC must use MinIO. The
upstream MinIO project archived its repository on 25 April 2026 and stopped
publishing community container images. The final community source release
available for this POC is `RELEASE.2025-10-15T17-29-55Z`, which includes the
upstream security fix described in that release.

The old upstream container tags cannot be pulled from the configured registries
in this environment. Unofficial mirrors would weaken provenance, so the local
image is built from the pinned official source release with the pinned Go 1.24.8
builder. This keeps the POC on MinIO without depending on a third-party rebuild.

## Decision

Run a single-node MinIO server under Podman Compose and persist its `/data` on a
named volume. Store Iceberg data and metadata in the `pcl-warehouse` bucket. The
MinIO API is available inside the Compose network at `http://minio:9000`; the
host API and console ports are configurable through `.env`.

Trino and the JVM application use path-style S3 requests and credentials loaded
from the untracked `.env`. Rake creates the bucket idempotently before the
lakehouse tables are initialized. The Iceberg REST catalog points its warehouse
at `s3://pcl-warehouse/warehouse`; the catalog decision is recorded in ADR-009.

## Consequences

- The POC now exercises the S3-compatible storage path required by the SRS.
- A local source build replaces the unavailable prebuilt community image and
  adds a Go builder image to first-time setup.
- This MinIO source baseline is archived upstream. The POC documents the pinned
  release and does not claim that the component is maintained or production
  ready.
- Secrets stay in `.env` and in a generated, gitignored Trino catalog file.
- Podman volume reset removes MinIO data, including Iceberg history.
