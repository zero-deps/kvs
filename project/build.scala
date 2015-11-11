package mws.kvs

import sbt._
import sbt.Keys._

object Build extends sbt.Build{
  lazy val root = Project(
    id = "kvs",
    base = file("."),
    settings = defaultSettings ++ Seq(
      libraryDependencies ++=Seq(
        "com.playtech.mws" %% "rng" % "1.0-68-g0ca5bed",
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
