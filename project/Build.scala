/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */

package mws.rng

import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import com.typesafe.sbt.packager.archetypes._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.docker._
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import sbt.Keys._
import sbt._
import sbtprotobuf.{ProtobufPlugin => PB}

object Publish {
  val repo = Some(Resolver.url("MWS Nexus Repo Snapshot",
    new URL("http://nexus-mobile.ee.playtech.corp/nexus/content/repositories/mws-snapshots/")))

  lazy val nexusCredentials = Credentials("Sonatype Nexus Repository Manager",
    "nexus-mobile.ee.playtech.corp", "mws", "aG1reeshie")

  lazy val settings = Seq(publishTo := repo,
    publishArtifact in Test := false,
    publishArtifact in(Compile, packageBin) := true,
    publishArtifact in(Compile, packageDoc) := false,
    publishArtifact in(Compile, packageSrc) := false,
    isSnapshot in(Compile) := version.value.contains("-SNAPSHOT"),
    credentials += nexusCredentials)
}

object Dependencies {

  object Versions {
    val scalaVersion = "2.11.6"
    val scalaTestVersion = "2.2.1"
    val akkaVersion = "2.3.11"
  }

  object Compile {
    import mws.rng.Dependencies.Versions._

    val shapeless   = "com.chuusai" %% "shapeless" % "2.0.0"
    val scalaz      = "org.scalaz" %% "scalaz-core" % "7.1.0"

    val actor       = "com.typesafe.akka" %% "akka-actor" % akkaVersion
    val cluster     = "com.typesafe.akka" %% "akka-cluster" % akkaVersion
    //val cmetrics    = "com.typesafe.akka" %% "akka-cluster-metrics" % "2.4-SNAPSHOT"
    val remote      = "com.typesafe.akka" %% "akka-remote" % akkaVersion
    val kernel      = "com.typesafe.akka" %% "akka-kernel" % akkaVersion

    val config  = "com.typesafe"      % "config"      % "1.2.1"

    val uncommonsMath = "org.uncommons.maths"         % "uncommons-maths" % "1.2.2a" exclude("jfree", "jcommon") exclude("jfree", "jfreechart")      // ApacheV2
    val levelDB       = "org.iq80.leveldb"            % "leveldb"         % "0.7"         // ApacheV2
    val levelDBNative = "org.fusesource.leveldbjni"   % "leveldbjni-all"  % "1.8"         // New BSD
    val protobuf      = "com.google.protobuf"         % "protobuf-java"   % "2.5.0"       // New BSD


    // Test

    object Test {
      val scalatest     = "org.scalatest"     %% "scalatest"    % "2.1.5"
      val scalautils    = "org.scalautils"    %% "scalautils"   % "2.1.5" % "test"
      val testkit       = "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
      val multinodekit  = "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion
      //val remotetest    = "com.typesafe.akka" %% "akka-remote-tests" % akkaVersion % "test"
      val specs2        = "org.specs2" %% "specs2" % "2.3.12" % "test"
      //val junit         = "junit" % "junit" % "4.10" % "test"

      val metrics         = "com.codahale.metrics" % "metrics-core" % "3.0.1" % "test" // ApacheV2
      val metricsJvm      = "com.codahale.metrics" % "metrics-jvm"  % "3.0.1" % "test" // ApacheV2
      val latencyUtils    = "org.latencyutils"     % "LatencyUtils" % "1.0.3" % "test" // Free BSD
      val hdrHistogram    = "org.hdrhistogram"     % "HdrHistogram" % "1.1.4" % "test" // CC0
      val metricsAll      = Seq(metrics, metricsJvm, latencyUtils, hdrHistogram)

    }
  }

  import mws.rng.Dependencies.Compile._

  val akka = Seq(config, actor, kernel, remote, cluster,
    levelDB, levelDBNative,
    protobuf,
    Test.scalatest, Test.scalautils, Test.testkit, Test.specs2, Test.multinodekit) ++ Test.metricsAll
}

object Build extends sbt.Build {

  lazy val buildSettings = Seq(
    organization := "playtech",
    version      := "1.0-SNAPSHOT",
    scalaVersion := Dependencies.Versions.scalaVersion
  )

  lazy val root = Project(
    id = "rng",
    base = file("."),
    settings = SbtMultiJvm.multiJvmSettings ++ PB.protobufSettings ++ Publish.settings ++Seq(
      javaSource in PB.protobufConfig <<= (sourceDirectory in Compile)(_ / "java"),
      libraryDependencies ++= Dependencies.akka,
      mainClass in Compile := Some("mws.rng.RingApp"),
      Keys.fork in run := true,
      isSnapshot := true,
      compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
      compileOrder := CompileOrder.JavaThenScala,
      parallelExecution in Test := false,
      executeTests in Test <<= (executeTests in Test, executeTests in MultiJvm) map {
        case (testResults, multiNodeResults)  =>
          val overall =
            if (testResults.overall.id < multiNodeResults.overall.id)
              multiNodeResults.overall
            else
              testResults.overall
          Tests.Output(overall,
            testResults.events ++ multiNodeResults.events,
            testResults.summaries ++ multiNodeResults.summaries)
      },

      mappings in Docker <+= (defaultLinuxInstallLocation in Docker, sourceDirectory) map { (path,src) =>
        val conf = src / "main" / "resources" / "reference.conf"
        conf -> s"$path/conf/application.conf"
      },
      dockerExposedPorts in Docker := Seq(4334,9998),
      dockerEntrypoint in Docker := Seq("sh", "-c",
          "KAI_IP=`/sbin/ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{print $1}'` bin/rng $*"),
      dockerRepository := Some("playtech")
    )
  ).settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
  .configs(MultiJvm)
  .enablePlugins(AkkaAppPackaging, DockerPlugin)

  override lazy val settings = super.settings ++
    buildSettings ++
    Seq(shellPrompt := { s => Project.extract(s).currentProject.id + " > " })

  lazy val defaultSettings = Seq(
    scalacOptions in Compile ++= Seq("-encoding", "UTF-8", "-target:jvm-1.7", "-deprecation", "-feature", 
      "-unchecked", "-Xlog-reflective-calls", "-Xlint", "-Yclosure-elim", "-Yinline", "-Xverify", "-language:postfixOps"),
    javacOptions in compile ++= Seq("-encoding", "UTF-8", "-source", "1.7", "-target", "1.7", "-Xlint:unchecked", "-Xlint:deprecation"),
    incOptions := incOptions.value.withNameHashing(true)
  )
}
