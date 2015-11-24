package mws.kvs

import sbt._
import sbt.Keys._

object Build extends sbt.Build{
  lazy val root = Project(
    id = "kvs",
    base = file("."),
    settings = defaultSettings ++ Seq(
      libraryDependencies ++=Seq(
        ("com.playtech.mws" %% "rng" % "1.0-99-g4c53565").
          exclude("org.scalatest", "scalatest_2.11"),
        "net.sf.opencsv" % "opencsv" % "2.3",
        ("com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.5.2").
          exclude("org.scala-lang", "scala-reflect").
          exclude("com.fasterxml.jackson.core", "jackson-databind").
          exclude("org.scalatest", "scalatest_2.11"),
        ("com.fasterxml.jackson.core" % "jackson-databind" % "2.5.2").
          exclude("com.fasterxml.jackson.core", "jackson-annotations").
          exclude("com.fasterxml.jackson.core", "jackson-core"),
        "junit" % "junit" % "4.12" % "test",
        "org.scalatest"    %% "scalatest" % "2.2.4" % "test",
        "com.typesafe.akka" %% "akka-testkit" % "2.3.14" % "test")))

  lazy val defaultSettings = Defaults.coreDefaultSettings ++ Seq()

  lazy val buildSettings = Seq(
    organization := "mws",
    description := "Abstract Scala Types Key-Value Storage",
    version := "0.1",
    scalaVersion := "2.11.7")

  override lazy val settings = super.settings ++ buildSettings ++ resolverSettings ++ Seq(
    shellPrompt := (Project.extract(_).currentProject.id + " > "))

  lazy val resolverSettings = Seq(
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
    resolvers := Resolver.mavenLocal
      +: Seq("MWS Releases" at "http://ua-mws-nexus01.ee.playtech.corp/nexus/content/repositories/releases/"))
}
