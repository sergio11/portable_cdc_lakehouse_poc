// ---------------------------------------------------------------------------
// Portable CDC Lakehouse — build definition
// Single source of truth for every pinned version. See docs/compatibility-matrix.md
// ---------------------------------------------------------------------------

import sbtassembly.AssemblyPlugin.autoImport._

// Keep versions in one place; the Rakefile (rakelib/config.rake) mirrors these
// for container tags. Change both together and re-run `rake verify`.
val Versions = new
{
  val scala213   = "2.13.18"
  val beam       = "2.76.0"
  val flinkRun   = "2.2"     // Flink runner artifact family (Flink 2.2.x, Beam >= 2.75)
  val iceberg    = "1.11.0"  // kept in sync with the Beam 2.76 Iceberg dependency
  val scalaTest  = "3.2.19"

  // Wire-format parsing for the Debezium envelope lives in the adapter layer,
  // never in the domain, so the core stays dependency-free.
  val circe = "0.14.10"
}

ThisBuild / organization := "com.example"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := Versions.scala213

// Beam + supporting JVM toolchain. Scala sits on top of the Beam Java SDK (ADR-001).
// beam-sdks-java-io-kafka transitively references Confluent's schema-registry
// client + avro serializer, which are published to Confluent's repo, not Central.
// Beam declares this resolver itself; consumers must restate it.
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "org.apache.beam" % "beam-sdks-java-core"          % Versions.beam,
  "org.apache.beam" % "beam-runners-direct-java"     % Versions.beam,
  "org.apache.beam" % s"beam-runners-flink-${Versions.flinkRun}" % Versions.beam,
  "org.apache.beam" % "beam-sdks-java-io-kafka"      % Versions.beam,
  "org.apache.beam" % "beam-sdks-java-io-iceberg"    % Versions.beam,
  "org.apache.iceberg" % "iceberg-aws-bundle"         % Versions.iceberg,
  "io.circe" %% "circe-core"                        % Versions.circe,
  "io.circe" %% "circe-parser"                      % Versions.circe,
  "org.scalatest"  %% "scalatest"                    % Versions.scalaTest % Test
)

// Fail on warnings so incompatibilities surface at compile time, not at runtime.
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Wunused:imports"
)

// --- Fat-jar assembly (used to submit the same artifact to DirectRunner / Flink) ---
assembly / mainClass           := Some("com.example.pcl.app.CdcStreamingPipeline")
assembly / assemblyJarName     := "pcl-pipeline.jar"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _ @_*) => MergeStrategy.concat
  case PathList("META-INF", _ @_*)             => MergeStrategy.discard
  case "reference.conf"                        => MergeStrategy.concat
  case "application.conf"                      => MergeStrategy.concat
  case _                                       => MergeStrategy.first
}

// sbt-assembly is a global plugin; disable the default test grouping noise.
assembly / test := {}
