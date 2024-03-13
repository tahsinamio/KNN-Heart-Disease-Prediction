
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "testspark"
  )


val sparkVersion = "3.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalanlp" %% "breeze" % "1.2",
  "org.scalanlp" %% "breeze-viz" % "1.2",
  "org.jfree" % "jfreechart" % "1.5.3",
  "org.plotly-scala" %% "plotly-render" % "0.8.1",
  "org.scalafx" %% "scalafx" % "21.0.0-R32"
)
