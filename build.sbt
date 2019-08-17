
ThisBuild / scalaVersion     := "2.13.0"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.dataintoresults"
ThisBuild / organizationName := "DataIntoResults"

lazy val root = (project in file("."))
  .settings(
    name := "dataset-generator"
  )

