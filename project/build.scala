package kvs

import sbt._
import sbt.Keys._

object Versions {
  val scala = "2.11.8"
  val scalaz = "7.2.2"
  val pickling = "0.11.0-M2"
  val akka = "2.3.15"
  val rng = "1.0-192-g37b1870"
}

object Build extends sbt.Build {
  lazy val root = Project(
    id = "kvs",
    base = file("."),
    settings = Defaults.coreDefaultSettings ++ publishSettings ++ Seq(
      scalacOptions in Compile ++= Seq("-feature", "-deprecation", "-target:jvm-1.7"),
      javacOptions in Compile ++= Seq("-source", "1.7", "-target", "1.7"),
      fork in Test := true,
      libraryDependencies ++= Seq(
        "org.scalaz" %% "scalaz-core" % Versions.scalaz,

        ("org.scala-lang.modules" %% "scala-pickling" % Versions.pickling).
          exclude("org.scala-lang.modules","scala-parser-combinators_2.11").
          exclude("org.scala-lang","scala-compiler").
          exclude("org.scala-lang","scala-reflect"),
        ("org.scala-lang" % "scala-compiler" % Versions.scala).
          exclude("org.scala-lang.modules","scala-xml_2.11"),
        "org.scala-lang.modules" %% "scala-xml" % "1.0.5",

        "com.typesafe.akka" %% "akka-cluster" % Versions.akka % Provided,
        "com.playtech.mws" %% "rng" % Versions.rng,

        "org.scalatest" %% "scalatest" % "2.2.6" % Test,
        "com.typesafe.akka" %% "akka-testkit" % Versions.akka % Test
      )
    )
  )

  lazy val buildSettings = Seq(
    organization := "com.playtech.mws",
    description := "Abstract Scala Types Key-Value Storage",
    version := org.eclipse.jgit.api.Git.open(file(".")).describe().call(),
    scalaVersion := Versions.scala
  )

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
