package extract

import java.nio.file.Files
import java.sql.DriverManager
import org.scalatest.matchers.should.Matchers
import support.SparkSessionTestWrapper

class ExtractDatabaseSpec extends SparkSessionTestWrapper with Matchers {

  test("ExtractDatabase reads SQLite seed data from a temporary database") {
    // Arrange
    val dbFile = Files.createTempFile("etl-extract", ".db")
    Class.forName("org.sqlite.JDBC")
    val connection = DriverManager.getConnection(s"jdbc:sqlite:${dbFile.toAbsolutePath}")
    try {
      val statement = connection.createStatement()
      statement.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, full_name TEXT, email TEXT, department TEXT)")
      statement.execute("INSERT INTO employees VALUES (1, 'Avery Stone', 'avery.stone@corp.example', 'IT')")
      statement.execute("INSERT INTO employees VALUES (2, 'Zara Ali', 'zara.ali@corp.example', 'Finance')")
      statement.close()
    } finally {
      connection.close()
    }

    // Act
    val df = ExtractDatabase.read(spark, ExtractDatabase.SQLiteConfig(dbFile.toString), "employees")

    // Assert
    df.count() shouldEqual 2L
    df.columns should contain allOf ("id", "full_name", "email", "department")
  }
}
