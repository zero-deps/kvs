val scalav = "3.1.2-RC3"
val zio = "1.0.13"
val akka = "2.6.19"
val rocks = "7.0.4"
val protoj = "3.20.0"
val lucene = "8.11.1"

lazy val kvsroot = project.in(file(".")).settings(
  scalaVersion := scalav
, libraryDependencies ++= Seq(
    "dev.zio" %% "zio-test-sbt" % zio % Test
  , "com.typesafe.akka" %% "akka-cluster-sharding" % akka
  )
, testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
, scalacOptions ++= scalacOptionsCommon
, Test / fork := true
, run / fork := true
, run / connectInput := true
).dependsOn(feed, search).aggregate(ring, feed, search)

lazy val ring = project.in(file("ring")).settings(
  scalaVersion := scalav
, Compile / scalaSource := baseDirectory.value / "src"
, libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-cluster" % akka
  , "org.rocksdb" % "rocksdbjni" % rocks
  , "dev.zio" %% "zio" % zio
  )
, scalacOptions ++= scalacOptionsCommon :+ "-nowarn"
).dependsOn(proto)

lazy val feed = project.in(file("feed")).settings(
  scalaVersion := scalav
, Compile / scalaSource := baseDirectory.value / "src"
, libraryDependencies ++= Seq(
    "dev.zio" %% "zio-streams" % zio
  , "com.typesafe.akka" %% "akka-cluster-sharding" % akka
  )
, scalacOptions ++= scalacOptionsCommon
).dependsOn(ring)

lazy val search = project.in(file("search")).settings(
  scalaVersion := scalav
, Compile / scalaSource := baseDirectory.value / "src"
, libraryDependencies ++= Seq(
    "dev.zio" %% "zio-streams" % zio
  , "com.typesafe.akka" %% "akka-cluster-sharding" % akka
  , "org.apache.lucene" % "lucene-analyzers-common" % lucene
  )
, scalacOptions ++= scalacOptionsCommon
).dependsOn(ring)

lazy val proto = project.in(file("deps/proto/proto")).settings(
  scalaVersion := scalav
, crossScalaVersions := scalav :: Nil
, libraryDependencies ++= Seq(
    "com.google.protobuf" % "protobuf-java" % protoj
  )
, scalacOptions ++= scalacOptionsCommon :+ "-Xcheck-macros"
).dependsOn(protosyntax)

lazy val protosyntax = project.in(file("deps/proto/syntax")).settings(
  scalaVersion := scalav
, crossScalaVersions := scalav :: Nil
, scalacOptions ++= scalacOptionsCommon
)

val scalacOptionsCommon = Seq(
  "release", "18"
, "-source:future"
, "-deprecation"
// , "-Yexplicit-nulls"
// , "-language:strictEquality"
)

Global / onChangedBuildSource := ReloadOnSourceChanges
