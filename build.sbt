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

      // Testing
      "org.scalameta" %% "munit" % "1.0.0" % Test
    )
  )

  Compile / run /fork := true

  run / javaOptions ++= Seq(
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED", 
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "-Xmx2G", 
    "-Dspark.ui.enabled=false"
  )
