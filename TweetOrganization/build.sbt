name := "TweetOrganization"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
  "org.xerial.snappy" % "snappy-java" % "1.1.2.6",
  "org.apache.spark" % "spark-sketch_2.11" % "2.0.0"

)


libraryDependencies += "org.json" % "json" % "20160810"