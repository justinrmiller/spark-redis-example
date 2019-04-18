name := "spark-redis-example"

val sparkVersion = "2.3.0"

version := "0.1"

organization := "com.justinrmiller"

scalaVersion := "2.11.12"

test in assembly := {}

fork in run := true

resolvers ++= Seq(
  Resolver.typesafeRepo("releases"),
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases",
  "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Twitter Repo" at "http://maven.twttr.com",
  "eaio.com" at "http://eaio.com/maven2"
)

libraryDependencies ++= Seq(
  "com.typesafe"      % "config"                          % "1.3.0",
  "ch.qos.logback"    % "logback-classic"                 % "1.1.3",

  "org.apache.spark"  % "spark-streaming_2.11"            % sparkVersion,
  "org.apache.spark"  % "spark-sql_2.11"                  % sparkVersion,

  "com.redislabs" % "spark-redis" % "2.3.1" exclude("com.redislabs", "jedis"),
  "com.redislabs" % "jedis" % "3.0.0-20181113.105826-9" from "https://oss.sonatype.org/content/repositories/snapshots/com/redislabs/jedis/3.0.0-SNAPSHOT/jedis-3.0.0-20181113.105826-9.jar"
)

mainClass in assembly := Some("com.justinrmiller.sparkstreaming.Main")

assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) { old =>
  {
    case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
    case x@PathList("META-INF", xs@_*) => old(x)
    case _ => MergeStrategy.last
  }
}
