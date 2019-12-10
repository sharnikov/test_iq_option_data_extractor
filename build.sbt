lazy val commonSettings = Seq(
  name := "Data Extractor",
  organization := "test.option.iq",
  scalaVersion := "2.11.12",
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
      "org.apache.spark" %% "spark-sql" % "2.2.0",
      "org.apache.spark" %% "spark-core" % "2.2.0"
    )
  )
