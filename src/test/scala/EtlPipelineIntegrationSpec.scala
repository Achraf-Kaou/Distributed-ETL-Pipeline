import config.PipelineConfig
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import org.scalatest.matchers.should.Matchers
import support.SparkSessionTestWrapper

class EtlPipelineIntegrationSpec extends SparkSessionTestWrapper with Matchers {

  private def baseConfig: String =
    Files.readString(Path.of("application.conf"), StandardCharsets.UTF_8)

  test("EtlPipeline runs end-to-end with local files and SQLite warehouse output") {
    // Arrange
    val tempDir = Files.createTempDirectory("etl-integration")
    val configFile = tempDir.resolve("application.test.conf")
    val configText =
      baseConfig +
        s"""
           |
           |etl.extract.apis = []
           |etl.extract.databases = []
           |etl.load.output-base-path = "${tempDir.resolve("output").toString.replace('\\', '/')}"
           |etl.load.warehouse-target {
           |  enabled = true
           |  db-type = "sqlite"
           |  sqlite-file-path = "${tempDir.resolve("warehouse/etl.db").toString.replace('\\', '/')}"
           |  schema = "main"
           |}
           |etl.audit.output-path = "${tempDir.resolve("audit").toString.replace('\\', '/')}"
           |etl.quarantine.base-path = "${tempDir.resolve("quarantine").toString.replace('\\', '/')}"
           |etl.orchestration.fail-fast = false
           |""".stripMargin
    Files.writeString(configFile, configText, StandardCharsets.UTF_8)
    val cfg = PipelineConfig.load(configFile.toString)

    // Act
    new EtlPipeline(spark, cfg).run()

    // Assert
    Files.exists(tempDir.resolve("audit/audit.log")) shouldBe true
    Files.exists(tempDir.resolve("warehouse/etl.db")) shouldBe true
    Files.list(tempDir.resolve("output/final")).findFirst().isPresent shouldBe true
  }
}
