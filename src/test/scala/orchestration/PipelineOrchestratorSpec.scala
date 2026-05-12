package orchestration

import audit.AuditLogger
import java.nio.file.Files
import logging.PipelineLogger
import org.scalatest.matchers.should.Matchers
import support.SparkSessionTestWrapper

class PipelineOrchestratorSpec extends SparkSessionTestWrapper with Matchers {

  import spark.implicits._

  private def newOrchestrator(failFast: Boolean): PipelineOrchestrator = {
    val auditFile = Files.createTempFile("audit", ".log")
    new PipelineOrchestrator(
      stages = Seq("extract", "clean"),
      failFast = failFast,
      logger = new PipelineLogger(metricsEnabled = true),
      auditLogger = new AuditLogger(auditFile.toString),
      runId = "test-run"
    )
  }

  test("PipelineOrchestrator executes enabled stages and returns results") {
    // Arrange
    val orchestrator = newOrchestrator(failFast = true)

    // Act
    val result = orchestrator.runStage("extract") {
      Seq((1, "a"), (2, "b")).toDF("id", "value")
    }

    // Assert
    result.get.count() shouldEqual 2L
    orchestrator.shouldRun("clean") shouldBe true
    orchestrator.shouldRun("load") shouldBe false
  }

  test("PipelineOrchestrator swallows stage failures when failFast is disabled") {
    // Arrange
    val orchestrator = newOrchestrator(failFast = false)

    // Act
    val result = orchestrator.runStage("extract") {
      throw new IllegalStateException("boom")
    }

    // Assert
    result shouldBe empty
  }

  test("PipelineOrchestrator propagates stage failures when failFast is enabled") {
    // Arrange
    val orchestrator = newOrchestrator(failFast = true)

    // Act / Assert
    val error = intercept[IllegalStateException] {
      orchestrator.runStage("extract") {
        throw new IllegalStateException("boom")
      }
    }

    error.getMessage shouldEqual "boom"
  }
}
