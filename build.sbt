name := "ot_simple_zeppelin_scala"
description := "OTL interpreter for Apache Zeppelin"
version := "1.2.1"
scalaVersion := "2.11.12"
crossPaths := false

credentials += Credentials(
 "Sonatype Nexus Repository Manager",
 sys.env.getOrElse("NEXUS_HOSTNAME", ""),
 sys.env.getOrElse("NEXUS_COMMON_CREDS_USR", ""),
 sys.env.getOrElse("NEXUS_COMMON_CREDS_PSW", "")
)

resolvers +=
  ("Sonatype OSS Snapshots" at "http://storage.dev.isgneuro.com/repository/ot.platform-sbt-releases/")
    .withAllowInsecureProtocol(true)

val dependencies = new {
 private val zeppelinVersion = "0.10.0"
 private val json4sVersion = "3.5.5"
 private val otSimpleConnectorScalaVersion = "1.1.0"
 private val scalatestVersion = "3.0.8"

 val zeppelin = "org.apache.zeppelin" % "zeppelin-interpreter" % zeppelinVersion % Compile
 val json4s = "org.json4s" %% "json4s-native" % json4sVersion % Compile
 val connectorScala = "com.isgneuro" % "ot_simple_connector_scala" % otSimpleConnectorScalaVersion % Compile
 val scalatest = "org.scalatest" %% "scalatest" % scalatestVersion % Test
}

libraryDependencies ++= Seq(
 dependencies.zeppelin,
 dependencies.json4s,
 dependencies.connectorScala,
 dependencies.scalatest
)



Test / parallelExecution := false

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

publishTo := Some(
 "Sonatype Nexus Repository Manager" at sys.env.getOrElse("NEXUS_OTP_URL_HTTPS", "")
   + "/repository/ot.platform-sbt-releases"
)
