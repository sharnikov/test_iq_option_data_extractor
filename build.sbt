lazy val commonSettings = Seq(
  name := "Data Extractor",
  organization := "test.option.iq",
  scalaVersion := "2.11.4",
  version := "0.1"
)

lazy val assamblySettings = Seq(
  mainClass in assembly := Some("test.option.iq.Extractor"),
  assemblyJarName in assembly := s"Extractor.jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case _ => MergeStrategy.last
  },
  assemblyShadeRules in assembly := Seq(
    ShadeRule.rename("org.apache.commons.io.**" -> "shadeio.@1").inAll
  )
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    assamblySettings,
    resourceDirectory := baseDirectory.value / "resources",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.4.4",
      "org.apache.spark" %% "spark-core" % "2.4.4",
      "org.postgresql" % "postgresql" % "42.2.9",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
      "com.databricks" %% "spark-csv" % "1.5.0",
      "org.slf4j" % "slf4j-simple" % "1.7.29",
      "com.typesafe" % "config" % "1.3.3",
      "com.iheart" %% "ficus" % "1.4.0",
    )
)
