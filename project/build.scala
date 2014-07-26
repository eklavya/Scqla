import sbt._
import Keys._

object ApplicationBuild extends Build {

  val appName         = "Scqla"
  val appVersion      = "1.0-SNAPSHOT"

  val appDependencies = Seq(
    // Add your project dependencies here,
    "com.typesafe.akka" %% "akka-actor" % "2.3.4",
    "com.typesafe" %% "config" % "1.2.0",
    "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
  )
}
