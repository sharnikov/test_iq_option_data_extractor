lazy val commonSettings = Seq(
  name := "Data Extractor",
  organization := "test.option.iq",
  scalaVersion := "2.12.0",
  version := "0.1"
)

lazy val assamblySettings = Seq(
  mainClass in assembly := Some("test.option.iq.Extractor"),
  assemblyJarName in assembly := s"Extractor.jar"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    assamblySettings,
    resourceDirectory := baseDirectory.value / "resources",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.0.0-preview",
      "org.apache.spark" %% "spark-core" % "3.0.0-preview",
      "org.postgresql" % "postgresql" % "42.2.9"
    )
)
