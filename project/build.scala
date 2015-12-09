package mws.kvs

import sbt._
import sbt.Keys._

object Build extends sbt.Build{
  lazy val root = Project(
    id = "kvs",
    base = file("."),
    settings = defaultSettings ++ publishSettings ++ Seq(
      libraryDependencies ++=Seq(
        "org.scalaz" %% "scalaz-core" % "7.2.0",
        "com.twitter" %% "bijection-core" % "0.8.1",
        "org.scala-lang.modules" %% "scala-pickling" % "0.10.1",
        "net.sf.opencsv" % "opencsv" % "2.3",
        ("com.playtech.mws" %% "rng" % "1.0-99-g4c53565").exclude("org.scalatest", "scalatest_2.11"),
        "junit" % "junit" % "4.12" % "test",
        "org.scalatest"    %% "scalatest" % "2.2.4" % "test",
        "org.scalactic" %% "scalactic" % "2.2.4" % "test",
        "com.typesafe.akka" %% "akka-testkit" % "2.3.14" % "test")))

  lazy val defaultSettings = Defaults.coreDefaultSettings ++ Seq(
    fork := true,
    scalacOptions ++= Seq("-feature", "-deprecation"))

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

  lazy val publishSettings = Seq(
    isSnapshot := true,
    publishMavenStyle := true,
    publishLocal <<= publishM2,
    publishArtifact in (Compile, packageBin) := true,
    publishArtifact in (Compile, packageDoc) := false,
    publishArtifact in (Compile, packageSrc) := false,
    publishArtifact in Test := false
  )
}
