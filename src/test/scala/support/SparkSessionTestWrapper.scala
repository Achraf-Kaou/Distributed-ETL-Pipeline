package support

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

trait SparkSessionTestWrapper extends AnyFunSuite with BeforeAndAfterAll {
  @transient protected lazy val spark: SparkSession = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val _ = spark
    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    try {
      if (!spark.sparkContext.isStopped) {
        spark.stop()
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally {
      super.afterAll()
    }
  }

  protected def repoPath(relativePath: String): String =
    s"${System.getProperty("user.dir")}/$relativePath"
}
