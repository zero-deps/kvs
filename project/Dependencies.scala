package mws.rng

import sbt._

object Dependencies {

  object Versions {
    val scala = "2.11.8"
    val akka = "2.3.15"
  }

  object Compile {
    val cluster = "com.typesafe.akka" %% "akka-cluster" % Versions.akka % Provided

    val levelDB = ("org.iq80.leveldb" % "leveldb" % "0.7").exclude("com.google.guava","guava")
    val levelDBNative = "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8"

    val test = Seq(
      "org.scalatest"     %% "scalatest"    % "2.1.5",
      "org.scalautils"    %% "scalautils"   % "2.1.5",
      "com.typesafe.akka" %% "akka-testkit" % Versions.akka,
      "com.typesafe.akka" %% "akka-multi-node-testkit" % Versions.akka,
      "org.specs2" %% "specs2" % "2.3.12",
      "com.codahale.metrics" % "metrics-core" % "3.0.1",
      "com.codahale.metrics" % "metrics-jvm"  % "3.0.1",
      "org.latencyutils"     % "LatencyUtils" % "1.0.3",
      "org.hdrhistogram"     % "HdrHistogram" % "1.1.4"
    ).map(_ % Test)
  }

  import Compile._

  val rng = Seq(cluster,levelDB,levelDBNative) ++ test
}
