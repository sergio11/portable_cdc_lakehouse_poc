// ---------------------------------------------------------------------------
// Portable CDC Lakehouse — build definition
// Pinned build versions.
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
  val kafka      = "3.9.1"   // matches the pinned broker and KafkaIO wire client
  val hadoop     = "3.3.6"  // Beam IcebergIO loads Hadoop Configuration at runtime
  val scalaTest  = "3.2.19"

  // Wire-format parsing for the Debezium envelope lives in the adapter layer,
  // never in the domain, so the core stays dependency-free.
  val circe = "0.14.10"
}

ThisBuild / organization := "com.example"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := Versions.scala213

// Scala uses Beam's Java SDK for the pipeline runtime.
// beam-sdks-java-io-kafka transitively references Confluent's schema-registry
// client + avro serializer, which are published to Confluent's repo, not Central.
// Beam declares this resolver itself; consumers must restate it.
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "org.apache.beam" % "beam-sdks-java-core"          % Versions.beam,
  "org.apache.beam" % "beam-runners-direct-java"     % Versions.beam,
  "org.apache.beam" % s"beam-runners-flink-${Versions.flinkRun}" % Versions.beam,
  "org.apache.beam" % "beam-sdks-java-io-kafka"      % Versions.beam,
  "org.apache.kafka" % "kafka-clients"               % Versions.kafka,
  "org.apache.beam" % "beam-sdks-java-io-iceberg"    % Versions.beam,
  "org.apache.iceberg" % "iceberg-aws"                % Versions.iceberg,
  "org.apache.iceberg" % "iceberg-aws-bundle"         % Versions.iceberg,
  "org.apache.hadoop" % "hadoop-client-api"            % Versions.hadoop,
  "org.apache.hadoop" % "hadoop-client-runtime"        % Versions.hadoop,
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
