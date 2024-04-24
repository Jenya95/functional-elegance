name := "functional-elegance"

version := "0.1"

scalaVersion := "2.13.13"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.4",
  "org.apache.spark" %% "spark-sql" % "3.3.4",
  "org.typelevel" %% "cats-core" % "2.10.0",
  "com.lihaoyi" %% "pprint" % "0.7.0"
)

run / fork := true
