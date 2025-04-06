name := "pet_projects"

version := "0.1"

scalaVersion := "2.13.8"


lazy val sparkVersion = "3.2.1"

lazy val circeVersion = "0.14.3"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
)