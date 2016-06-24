package kvs

import sbt._
import sbt.Keys._

object Versions {
  val scala = "2.11.8"
  val scalaz = "7.2.2"
  val pickling = "0.11.0-M2"
  val rng = "1.0-189-gd8ffa57"
}

object Build extends sbt.Build{
  lazy val root = Project(
    id = "kvs",
    base = file("."),
    settings = Defaults.coreDefaultSettings ++ publishSettings ++ Seq(
      scalacOptions in Compile ++= Seq("-feature", "-deprecation", "-target:jvm-1.7"),
      javacOptions in Compile ++= Seq("-source", "1.7", "-target", "1.7"),
      dependencyOverrides ++= Set(
        "org.scala-lang" % "scala-compiler" % Versions.scala,
        "org.scala-lang" % "scala-reflect" % Versions.scala,
        "org.scala-lang" % "scala-library" % Versions.scala
      ),
      libraryDependencies ++= Seq(
        "org.scalaz" %% "scalaz-core" % Versions.scalaz,
        ("org.scala-lang.modules" %% "scala-pickling" % Versions.pickling).
          exclude("org.scala-lang.modules","scala-parser-combinators_2.11"),
        "com.playtech.mws" %% "rng" % Versions.rng,
        "junit" % "junit" % "4.12" % Test,
        "org.scalatest" %% "scalatest" % "2.2.4" % Test,
        "com.typesafe.akka" %% "akka-testkit" % "2.3.14" % Test
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
