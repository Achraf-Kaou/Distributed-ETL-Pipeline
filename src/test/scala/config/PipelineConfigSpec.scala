package config

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PipelineConfigSpec extends AnyFunSuite with Matchers {

  private def baseConfig: String =
    Files.readString(Path.of("application.conf"), StandardCharsets.UTF_8)

  test("PipelineConfig.load parses the default application config") {
    // Arrange / Act
    val cfg = PipelineConfig.load("application.conf")

    // Assert
    cfg.spark.appName shouldEqual "Distributed ETL Pipeline"
    cfg.load.warehouseTarget.dbType shouldEqual "sqlite"
    cfg.orchestration.stages should contain ("build_star")
    cfg.quarantine.basePath shouldEqual "output/quarantine"
  }

  test("PipelineConfig.load applies overrides from a custom config file") {
    // Arrange
    val tempFile = Files.createTempFile("etl-config", ".conf")
    val configText =
      baseConfig +
        s"""
           |
           |etl.extract.flat-files.path = "data/raw/enterprise/csv"
           |etl.extract.apis = []
           |etl.extract.databases = []
           |etl.load.output-base-path = "${tempFile.getParent.resolve("output").toString.replace('\\', '/')}"
           |etl.audit.output-path = "${tempFile.getParent.resolve("audit").toString.replace('\\', '/')}"
           |etl.quarantine.base-path = "${tempFile.getParent.resolve("quarantine").toString.replace('\\', '/')}"
           |""".stripMargin
    Files.writeString(tempFile, configText, StandardCharsets.UTF_8)

    // Act
    val cfg = PipelineConfig.load(tempFile.toString)

    // Assert
    cfg.extract.flatFiles.path shouldEqual "data/raw/enterprise/csv"
    cfg.extract.databases shouldBe empty
    cfg.audit.outputPath should include ("audit")
    cfg.quarantine.basePath should include ("quarantine")
  }

  test("PipelineConfig.load fails fast on invalid join key mapping") {
    // Arrange
    val tempFile = Files.createTempFile("etl-invalid-config", ".conf")
    val configText =
      baseConfig +
        """
          |
          |etl.extract.apis = []
          |etl.extract.databases = []
          |etl.transform.joins = [{
          |  name = "broken-join"
          |  enabled = true
          |  right-source-type = "flat"
          |  right-path-or-query = "data/raw/departments.csv"
          |  join-type = "left"
          |  key-columns = []
          |  key-mappings = [{ left = "department", right = "" }]
          |  select-columns = ["department"]
          |  left-prefix = ""
          |  right-prefix = ""
          |  verbose = false
          |}]
          |""".stripMargin
    Files.writeString(tempFile, configText, StandardCharsets.UTF_8)

    // Act / Assert
    val error = intercept[IllegalArgumentException] {
      PipelineConfig.load(tempFile.toString)
    }

    error.getMessage should include ("Invalid key-mappings entry")
  }
}
