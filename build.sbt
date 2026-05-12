ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "DistributedEtlPipeline",

    libraryDependencies ++= Seq(

      // Spark Core
      "org.apache.spark" %% "spark-core" % "3.5.1",

      // Spark SQL
      "org.apache.spark" %% "spark-sql" % "3.5.1",

      // Excel Support
      "com.crealytics"   %% "spark-excel"    % "3.5.0_0.20.3",

      // xml Support
      "com.databricks"   %% "spark-xml"      % "0.18.0",
      
      // HTTP Client for API calls 
      "com.lihaoyi"      %% "requests"       % "0.9.0",

      // Application config (HOCON)
      "com.typesafe"      % "config"         % "1.4.3",

      // JDBC Drivers
      "org.postgresql" %  "postgresql"           % "42.7.3",
      "mysql"          %  "mysql-connector-java" % "8.0.33",
      "org.xerial"     %  "sqlite-jdbc"          % "3.45.3.0",

      // Logging
      "org.apache.logging.log4j" % "log4j-api" % "2.23.1",
      "org.apache.logging.log4j" % "log4j-core" % "2.23.1",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.23.1", // if using SLF4J bridges
      // JSON layout
      "org.apache.logging.log4j" % "log4j-layout-template-json" % "2.23.1",

      // Testing
      "org.scalameta" %% "munit" % "1.0.0" % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    )
  )

  Compile / run /fork := true

  run / javaOptions ++= Seq(
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED", 
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "-Xmx2G", 
    "-Dspark.ui.enabled=false"
  )

  Test / fork := true
  Test / parallelExecution := false
  Test / javaOptions ++= Seq(
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "-Xmx2G",
    "-Dspark.ui.enabled=false"
  )
