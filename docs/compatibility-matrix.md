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

Tags are pinned by digest where the project depends on exact behaviour. No
`latest` tags are permitted anywhere in the configuration.

| Service | Version | Validation |
|---|---|---|
| PostgreSQL | 17 | Pending — Phase 1 |
| Debezium PostgreSQL connector | 3.x | Pending — Phase 1 |
| Kafka (KRaft) | 3.9.x / 4.x (tbd at Phase 1) | Pending — Phase 1 |
| MinIO | release tag (tbd at Phase 1) | Pending — Phase 1 |
| Iceberg REST catalog | 1.11.0 | Pending — Phase 1 |
| DuckDB (query/inspection) | latest stable | Pending — Phase 1 |

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