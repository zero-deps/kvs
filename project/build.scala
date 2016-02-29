package mws.kvs

import sbt._
import sbt.Keys._

object Build extends sbt.Build{
  lazy val root = Project(
    id = "kvs",
    base = file("."),
    settings = defaultSettings ++ publishSettings ++ Seq(
      libraryDependencies ++= Seq(
        "org.scalaz" %% "scalaz-core" % "7.2.0",
        "org.scala-lang.modules" %% "scala-pickling" % "0.10.1",
        "com.playtech.mws" %% "rng" % "1.0-129-g32a44a5",
        "junit" % "junit" % "4.12" % Test,
        "org.scalatest" %% "scalatest" % "2.2.4" % Test,
        "com.typesafe.akka" %% "akka-testkit" % "2.3.14" % Test
      )
    )
  )

  lazy val defaultSettings = Defaults.coreDefaultSettings ++ Seq(
    fork := true,
    scalacOptions ++= Seq("-feature", "-deprecation"))

  lazy val buildSettings = Seq(
    organization := "com.playtech.mws",
    description := "Abstract Scala Types Key-Value Storage",
    version := org.eclipse.jgit.api.Git.open(file(".")).describe().call(),
    scalaVersion := "2.11.7")

  override lazy val settings = super.settings ++ buildSettings ++ resolverSettings ++ Seq(
    shellPrompt := (Project.extract(_).currentProject.id + " > "))

  lazy val resolverSettings = Seq(
    resolvers ++= Seq(
      Resolver.mavenLocal,
      "MWS Releases Resolver" at "http://ua-mws-nexus01.ee.playtech.corp/nexus/content/repositories/releases/"
    )
  )

  lazy val publishSettings = Seq(
    publishTo := Some("MWS Releases" at "http://ua-mws-nexus01.ee.playtech.corp/nexus/content/repositories/releases"),
    credentials += Credentials("Sonatype Nexus Repository Manager", "ua-mws-nexus01.ee.playtech.corp", "wpl-deployer", "aG1reeshie"),
    publishArtifact := true,
    publishArtifact in Compile := true,
    publishArtifact in Test := false,
    publishMavenStyle := true,
    pomIncludeRepository := (_ => false),
    publishLocal <<= publishM2,
    isSnapshot := true
  )
}
