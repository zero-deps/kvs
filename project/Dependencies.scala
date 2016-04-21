package mws.rng

import sbt._

object Dependencies {

  object Versions {
    val scalaVersion = "2.11.8"
    val scalaTestVersion = "2.2.1"
    val akkaVersion = "2.3.15"
  }

  object Compile {
    import mws.rng.Dependencies.Versions._

    val shapeless   = "com.chuusai" %% "shapeless" % "2.0.0"
    val scalaz      = "org.scalaz" %% "scalaz-core" % "7.1.0"

    val actor       = "com.typesafe.akka" %% "akka-actor" % akkaVersion
    val cluster     = "com.typesafe.akka" %% "akka-cluster" % akkaVersion
    val remote      = "com.typesafe.akka" %% "akka-remote" % akkaVersion
    val kernel      = "com.typesafe.akka" %% "akka-kernel" % akkaVersion

    val config  = "com.typesafe"      % "config"      % "1.2.1"

    val uncommonsMath = "org.uncommons.maths"         % "uncommons-maths" % "1.2.2a" exclude("jfree", "jcommon") exclude("jfree", "jfreechart")      // ApacheV2
    val levelDB       = "org.iq80.leveldb"            % "leveldb"         % "0.7"         // ApacheV2
    val levelDBNative = "org.fusesource.leveldbjni"   % "leveldbjni-all"  % "1.8"         // New BSD
    val protobuf      = "com.google.protobuf"         % "protobuf-java"   % "2.5.0"       // New BSD

    object Test {
      val scalatest     = "org.scalatest"     %% "scalatest"    % "2.1.5" % "test"
      val scalautils    = "org.scalautils"    %% "scalautils"   % "2.1.5" % "test"
      val testkit       = "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
      val multinodekit  = "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test"
      val specs2        = "org.specs2" %% "specs2" % "2.3.12" % "test"
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
