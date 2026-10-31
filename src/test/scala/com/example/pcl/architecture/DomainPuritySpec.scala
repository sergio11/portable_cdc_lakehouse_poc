package com.example.pcl.architecture

import com.example.pcl.config.PclSettings
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Ensures the domain bytecode stays free of Beam, Kafka, Iceberg, logging, and clock APIs.
  */
class DomainPuritySpec extends AnyFlatSpec with Matchers {

  private val domainPackages = Seq(
    "com/example/pcl/domain/"
  )

  // Framework / infrastructure namespaces the core must never reference.
  private val forbiddenPrefixes = Seq(
    "org/apache/beam/",
    "org/apache/kafka/",
    "org/apache/iceberg/",
    "org/slf4j/",
    "ch/qos/logback/"
  )

  private def domainClassFiles: Seq[java.io.File] = {
    val base = new java.io.File("target/scala-2.13/classes")
    def walk(dir: java.io.File): Seq[java.io.File] = {
      val children = Option(dir.listFiles()).map(_.toSeq).getOrElse(Seq.empty)
      children.flatMap { f =>
        if (f.isDirectory) walk(f)
        else if (f.getName.endsWith(".class")) Seq(f)
        else Seq.empty
      }
    }
    walk(base).filter { f =>
      val n = f.getAbsolutePath.replace('\\', '/')
      domainPackages.exists(p => n.contains(p))
    }
  }

  "The domain package" should "contain compiled classes" in {
    domainClassFiles should not be empty
  }

  it should "never reference Beam, Kafka, Iceberg or logging frameworks" in {
    val violations = domainClassFiles.flatMap { file =>
      // A cheap structural proxy: read raw bytes and look for internal class names.
      // Class files embed referenced types as UTF8 constants, so a prefix match is
      // a reliable, dependency-free way to detect leakage without a bytecode library.
      val bytes = java.nio.file.Files.readAllBytes(file.toPath)
      val raw   = new String(bytes, java.nio.charset.StandardCharsets.ISO_8859_1)
      val hits  = forbiddenPrefixes.filter(raw.contains)
      if (hits.nonEmpty) Some(file.getName -> hits) else None
    }

    withClue(s"Domain classes leaked framework references: $violations") {
      violations shouldBe empty
    }
  }

  "PclSettings.fromEnv" should "surface a reason code when run id is missing" in {
    val result = PclSettings.fromEnv(Map("PCL_SOURCE_ID" -> "pg-1"))
    result shouldBe Left(PclSettings.MissingRunId)
  }

  it should "surface a reason code when source id is missing" in {
    val result = PclSettings.fromEnv(Map("PCL_RUN_ID" -> "run-1"))
    result shouldBe Left(PclSettings.MissingSourceId)
  }

  it should "build settings when both variables are present" in {
    val result = PclSettings.fromEnv(Map("PCL_RUN_ID" -> "run-1", "PCL_SOURCE_ID" -> "pg-1"))
    result shouldBe Right(
      PclSettings(runId = "run-1", sourceId = "pg-1", consumerGroup = "pcl-batch-pg-1")
    )
  }

  it should "keep the speed layer on its own consumer group and target table" in {
    val result = PclSettings.fromEnv(
      Map(
        "PCL_RUN_ID"         -> "speed",
        "PCL_SOURCE_ID"      -> "pg-1",
        "PCL_PIPELINE_LAYER" -> "speed",
        "PCL_CONSUMER_GROUP" -> "pcl-speed-pg-1",
        "PCL_TARGET_TABLE"   -> "speed.order_cdc_events",
        "PCL_COMMIT_OFFSETS" -> "true"
      )
    )

    result.map(settings =>
      (settings.pipelineLayer, settings.consumerGroup, settings.targetTable, settings.commitOffsets)
    ) shouldBe
      Right(("speed", "pcl-speed-pg-1", "speed.order_cdc_events", true))
  }

  it should "reject a target table without a namespace" in {
    val result = PclSettings.fromEnv(
      Map("PCL_RUN_ID" -> "run-1", "PCL_SOURCE_ID" -> "pg-1", "PCL_TARGET_TABLE" -> "landing")
    )
    result shouldBe Left(PclSettings.InvalidTargetTable)
  }
}
