name := "spark-svd"

version := "1.0"

crossScalaVersions := Seq("2.10.6", "2.11.8")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "latest.integration" % Provided,
  "org.apache.spark" %% "spark-mllib" % "latest.integration" % Provided,
  "org.apache.spark" %% "spark-hive" % "latest.integration" % Provided
)