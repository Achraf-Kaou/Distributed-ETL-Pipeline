package transform

import org.scalatest.matchers.should.Matchers
import support.SparkSessionTestWrapper

class TransformJoinSpec extends SparkSessionTestWrapper with Matchers {

  import spark.implicits._

  test("TransformJoin joins with key mappings, prefixes, and explicit selects") {
    // Arrange
    val employees = Seq(
      ("sales", "amelia.hart@northwind.com", 91000.0),
      ("finance", "leila.benali@corp.example", 79000.0)
    ).toDF("department", "email", "salary")

    val departments = Seq(
      ("sales", "Global Sales", "New York, USA"),
      ("finance", "Corporate Finance", "Paris")
    ).toDF("dept_id", "dept_name", "location")

    val config = TransformJoin.JoinConfig(
      joinType = "left",
      keyColumns = Seq.empty,
      keyMappings = Seq("department" -> "dept_id"),
      selectColumns = Seq("department", "email", "salary", "dim_dept_name", "dim_location"),
      rightPrefix = "dim_",
      verbose = false
    )

    // Act
    val result = TransformJoin.join(employees, departments, config)

    // Assert
    result.columns shouldEqual Array("department", "email", "salary", "dim_dept_name", "dim_location")
    result.count() shouldEqual 2L
    result.head().getAs[String]("dim_dept_name") should not be empty
  }

  test("TransformJoin fails fast when a key is missing") {
    // Arrange
    val left = Seq((1, "a")).toDF("id", "value")
    val right = Seq((1, "b")).toDF("other_id", "value")

    // Act / Assert
    val error = intercept[IllegalArgumentException] {
      TransformJoin.join(left, right, TransformJoin.JoinConfig("inner", Seq("id"), verbose = false))
    }

    error.getMessage should include ("RIGHT")
  }
}
