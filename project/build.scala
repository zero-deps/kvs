package kvs

import sbt._
import sbt.Keys._

object Versions {
  val scala = "2.11.8"
  val scalaz = "7.2.6"
  val pickling = "0.11.0-M2"
  val akka = "2.4.10-12-g84649db"
  val xml = "1.0.5"
  val scalatest = "3.0.0"
  val levelDB = "0.7"
  val levelDBNative = "1.8"
}

object Build extends sbt.Build {
  lazy val root = Project(
    id = "kvs",
    base = file("."),
    settings = Defaults.coreDefaultSettings ++ publishSettings ++ Seq(
      scalacOptions in Compile ++= Seq("-feature","-deprecation"),
      fork in Test := true,
      libraryDependencies ++= Seq(
        "org.scalaz" %% "scalaz-core" % Versions.scalaz,
        ("org.scala-lang.modules" %% "scala-pickling" % Versions.pickling).
          exclude("org.scala-lang.modules","scala-parser-combinators_2.11").
          exclude("org.scala-lang","scala-compiler").
          exclude("org.scala-lang","scala-reflect"),
        ("org.scala-lang" % "scala-compiler" % Versions.scala).
          exclude("org.scala-lang.modules","scala-xml_2.11"),
        "org.scala-lang.modules" %% "scala-xml" % Versions.xml,
        "com.playtech.mws.akka" %% "akka-cluster" % Versions.akka,
        "org.iq80.leveldb" % "leveldb" % Versions.levelDB,
        "org.fusesource.leveldbjni"   % "leveldbjni-all" % Versions.levelDBNative,
        "org.scalatest" %% "scalatest" % Versions.scalatest % Test,
        "com.playtech.mws.akka" %% "akka-testkit" % Versions.akka % Test
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
      "releases resolver" at "http://nexus.mobile.playtechgaming.com/nexus/content/repositories/releases"
    )
  )

  lazy val publishSettings = Seq(
    publishTo := Some("releases" at "http://nexus.mobile.playtechgaming.com/nexus/content/repositories/releases"),
    credentials += Credentials("Sonatype Nexus Repository Manager","nexus.mobile.playtechgaming.com","wpl-deployer","aG1reeshie"),
    publishArtifact := true,
    publishArtifact in Compile := true,
    publishArtifact in Test := false,
    publishMavenStyle := true,
    pomIncludeRepository := (_ => false),
    publishLocal <<= publishM2,
    isSnapshot := true
  )
}
