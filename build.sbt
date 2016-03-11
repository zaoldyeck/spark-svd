name := "spark-svd"

version := "1.0"

crossScalaVersions := Seq("2.10.6", "2.11.7")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "latest.integration",
  "org.apache.spark" %% "spark-mllib" % "latest.integration",
  "org.apache.spark" %% "spark-hive" % "latest.integration"
)