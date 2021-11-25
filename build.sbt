val scalav = "3.1.1-RC1-bin-20211007-c041327-NIGHTLY"
val zio = "1.0.12"
val akka = "2.6.17"
val rocks = "6.26.1"
val protoj = "4.0.0-rc-2"

lazy val kvsroot = project.in(file(".")).settings(
  scalaVersion := scalav
, libraryDependencies ++= Seq(
    "dev.zio" %% "zio-test-sbt" % zio % Test
  , "com.typesafe.akka" %% "akka-cluster-sharding" % akka
  )
, testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
, scalacOptions ++= scalacOptionsStrict
, Test / fork := true
, run / fork := true
, run / connectInput := true
).dependsOn(feed).aggregate(feed)

lazy val feed = project.in(file("feed")).settings(
  scalaVersion := scalav
, Compile / scalaSource := baseDirectory.value / "src"
, libraryDependencies ++= Seq(
    "dev.zio" %% "zio-streams" % zio
  , "com.typesafe.akka" %% "akka-cluster-sharding" % akka
  )
, scalacOptions ++= scalacOptionsBase // Strict
).dependsOn(ring)

lazy val ring = project.in(file("ring")).settings(
  scalaVersion := scalav
, Compile / scalaSource := baseDirectory.value / "src"
, libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-cluster" % akka
  , "org.rocksdb" % "rocksdbjni" % rocks
  , "dev.zio" %% "zio" % zio
  )
, scalacOptions ++= scalacOptionsBase :+ "-nowarn"
).dependsOn(proto)

lazy val proto = project.in(file("deps/proto/proto")).settings(
  scalaVersion := scalav
, crossScalaVersions := scalav :: Nil
, libraryDependencies ++= Seq(
    "com.google.protobuf" % "protobuf-java" % protoj
  )
, scalacOptions ++= scalacOptionsBase :+ "-Xcheck-macros"
).dependsOn(protosyntax)

lazy val protosyntax = project.in(file("deps/proto/syntax")).settings(
  scalaVersion := scalav
, crossScalaVersions := scalav :: Nil
, scalacOptions ++= scalacOptionsStrict
)

val scalacOptionsBase = Seq(
  "-encoding", "UTF-8"
, "release", "17"
, "-source:future"
, "-deprecation"
, "-Yexplicit-nulls"
)

val scalacOptionsStrict = scalacOptionsBase ++ Seq(
  "-language:strictEquality"
)

Global / onChangedBuildSource := ReloadOnSourceChanges
