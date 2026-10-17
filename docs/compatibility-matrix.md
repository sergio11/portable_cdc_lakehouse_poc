# Compatibility Matrix

Every version used by the POC is pinned and verified as a *set*. A change to any
entry here requires re-running `rake verify` and, for runtime components,
the corresponding integration or portability test.

Legend for **Validation**:

- **Verified** — exercised in this repository with evidence below.
- **Pending** — resolved/locked but not yet executed at runtime; the phase that
  proves it is named.

## 1. JVM toolchain

The toolchain never touches the host: every build and run happens inside a
pinned container invoked by Rake. No local JVM, sbt or Scala install is required.

| Component | Version | Validation |
|---|---|---|
| Container runtime | Podman 5.8.2 | Verified |
| Compose provider | podman-compose 1.5.0 | Verified |
| Build image | `sbtscala/scala-sbt:eclipse-temurin-21.0.12_8_1.13.0_2.13.18` | Verified |
| Build image digest | `sha256:c848bfcb0acb4086609c9c83ac3e64743465f11b3a18ca282f69d65c15b32170` | Verified |
| JDK | Eclipse Temurin 21.0.12+8 | Verified |
| sbt | 1.13.0 | Verified |
| Scala | 2.13.18 | Verified |
| Ruby (orchestration) | 3.4.5 | Verified |
| Rake | 13.4.2 (pinned in `Gemfile.lock`) | Verified |

Evidence: `rake verify` compiles all sources and runs the domain plus
architecture suites inside the image.

## 2. Data plane libraries

| Component | Version | Validation |
|---|---|---|
| Apache Beam SDK | 2.76.0 | Verified |
| Beam DirectRunner | 2.76.0 (`beam-runners-direct-java`) | Verified |
| Beam FlinkRunner | 2.76.0 (`beam-runners-flink-2.2`) | Verified (classpath resolves) |
| Apache Flink | 2.2.x | Pending — Phase 6 |
| KafkaIO connector | 2.76.0 (`beam-sdks-java-io-kafka`) | Verified (resolves) |
| IcebergIO connector | 2.76.0 (`beam-sdks-java-io-iceberg`) | Verified (resolves) |
| Apache Iceberg | 1.11.0 | Pending — Phase 3 |
| ScalaTest | 3.2.19 | Verified |

### Why the Confluent resolver is declared

`beam-sdks-java-io-kafka` transitively references
`io.confluent:kafka-schema-registry-client` and `io.confluent:kafka-avro-serializer`,
which are published to Confluent's own repository rather than Maven Central.
Beam declares the resolver in its own build but does not propagate it to
consumers, so `build.sbt` restates it. Without this, dependency resolution fails
with `ResolveException`.

### JDK floor

IcebergIO is built against Iceberg 1.11.0 and therefore requires **JDK 17+**.
The pinned JDK 21 satisfies this.

## 3. Infrastructure services

Every image tag below was verified against its registry before being pinned. No
`latest` tags are permitted anywhere in the configuration.

| Service | Image / tag | Validation |
|---|---|---|
| PostgreSQL | `docker.io/library/postgres:17.2-bookworm` | Registry verified — runtime Pending (Phase 1) |
| Kafka (KRaft, single node) | `docker.io/apache/kafka:3.9.1` | Registry verified — runtime Pending (Phase 1) |
| Debezium / Connect | `docker.io/debezium/connect:3.2.4.Final` | Registry verified — runtime Pending (Phase 1) |
| Trino (SQL inspection) | `docker.io/trinodb/trino:476` | Registry verified — runtime Pending (Phase 1) |
| Apache Iceberg | 1.11.0 (via Beam) | Runtime Pending — Phase 3 |
| Iceberg catalog | HadoopCatalog on shared volume | ADR-009 |
| Object storage | Local filesystem volume | ADR-008 — MinIO withdrawn upstream |

### Object storage: MinIO is no longer publishable

The SRS specifies MinIO as the S3-compatible store. MinIO Inc. moved the
community edition to a *source-only* distribution in October 2025 and withdrew
the official images from Docker Hub and quay.io. Verified from this machine:

- `docker.io/minio/minio` → **401 Unauthorized** for every tag
- `quay.io/minio/minio` → pulls refused
- Last published image: `RELEASE.2025-09-07T16-13-09Z`

Third-party mirrors exist but were rejected as a supply-chain liability. The
warehouse therefore uses a local filesystem volume with HadoopCatalog; see
`docs/decisions/ADR-008-object-storage.md`.

### Iceberg REST catalog: no published image

A REST catalog was the preferred design, but no such image is published:

- `apache/iceberg-rest` → does not exist
- `apache/iceberg-rest-catalog` → does not exist
- `tabularium/iceberg-rest-catalog` → does not exist

HadoopCatalog is used instead, which is the path Beam's own `IcebergIO`
documentation recommends for local development. See
`docs/decisions/ADR-009-iceberg-catalog.md`.

### Why Trino instead of DuckDB

Trino's Iceberg connector supports the `hadoop` catalog type and the filesystem
warehouse as first-class, documented configurations. DuckDB's Iceberg extension
is less certain for a non-REST catalog, and the POC cannot launch containers to
empirically validate that path, so the lower-risk engine was chosen.

### Kafka addressing

Kafka advertises only `kafka:9092` and is deliberately **not** published to the
host. A single advertised address avoids the classic split where host clients and
container clients need different values. The Beam application joins the
`pcl_default` Compose network and uses the same address.

## 4. Known risk: Flink and the lz4 capability conflict

Beam release notes for Flink 2.1+ warn that Flink ships
`at.yawk.lz4:lz4-java` while Kafka clients depend on `org.lz4:lz4-java`; both
declare the `lz4-java` capability, which Gradle resolves as a capability conflict.
Phase 6 must either relocate one of them or confirm the sbt classpath is
unaffected. This is called out explicitly in `docs/decisions/` when resolved.

## 5. Environment notes

- Builds write to named volumes (`pcl-build-target`, `pcl-build-project`) rather
  than the bind-mounted working tree. The Windows/WSL bind mount rejects the
  file-timestamp writes sbt performs in its incremental caches, and native
  volumes are substantially faster.
- Coursier is mounted as a persistent volume. Without it every invocation
  re-verifies the full Beam/Flink/Iceberg classpath.
- The Podman machine used during development is a 4-CPU, 6 GiB WSL instance;
  absolute timings are not a meaningful performance signal for this POC.