package kvs

import sbt._
import sbt.Keys._

object Versions {
  val scala = "2.12.3"
  val scalaz = "7.2.15"
  val pickling = "1.0"
  val akka = "2.5.6"
  val logback = "1.2.3"
  val scalatest = "3.0.1"
  val leveldb = "1.8"
}

object Build extends sbt.Build {
  lazy val root = Project(
    id = "kvs",
    base = file("."),
    settings = Defaults.coreDefaultSettings ++ publishSettings ++ Seq(
      mainClass in (Compile,run) := Some("mws.kvs.Run"),
      cancelable in Global := true,
      fork in run := true,
      scalacOptions in Compile ++= Seq("-feature","-deprecation"/*,"-Xlog-implicits"*/),
      fork in Test := true,
      libraryDependencies ++= Seq(
        "org.scalaz"        %% "scalaz-core"             % Versions.scalaz,
        "org.fusesource.leveldbjni" % "leveldbjni-all"   % Versions.leveldb,
        "ch.qos.logback"     % "logback-classic"         % Versions.logback,

        "com.typesafe.akka" %% "akka-actor"              % Versions.akka,
        "com.typesafe.akka" %% "akka-cluster"            % Versions.akka,
        "com.typesafe.akka" %% "akka-cluster-sharding"   % Versions.akka,
        "com.typesafe.akka" %% "akka-cluster-tools"      % Versions.akka,
        "com.typesafe.akka" %% "akka-distributed-data"   % Versions.akka,
        "com.typesafe.akka" %% "akka-protobuf"           % Versions.akka,
        "com.typesafe.akka" %% "akka-remote"             % Versions.akka,
        "com.typesafe.akka" %% "akka-slf4j"              % Versions.akka,
        "com.typesafe.akka" %% "akka-stream"             % Versions.akka,

        "com.playtech.mws"  %% "scala-pickling"          % Versions.pickling  % Test,
        "org.scalatest"     %% "scalatest"               % Versions.scalatest % Test,
        "com.typesafe.akka" %% "akka-multi-node-testkit" % Versions.akka      % Test,
        "com.typesafe.akka" %% "akka-stream-testkit"     % Versions.akka      % Test,
        "com.typesafe.akka" %% "akka-testkit"            % Versions.akka      % Test
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
    publishMavenStyle := true,
    pomIncludeRepository := (_ => false),
    isSnapshot := true
  )
}
