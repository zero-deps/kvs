val scalav = "3.3.3"
val zio = "2.0.21"
val pekko = "1.0.2"
val rocks = "8.11.3"
val protoj = "3.25.3"
val lucene = "9.10.0"

lazy val kvsroot = project.in(file(".")).settings(
  scalaVersion := scalav
, libraryDependencies ++= Seq(
    "dev.zio" %% "zio-test-sbt" % zio % Test
  , "org.apache.pekko" %% "pekko-cluster-sharding" % pekko
  )
, testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
, scalacOptions ++= scalacOptionsCommon
, Test / fork := true
, run / fork := true
, run / connectInput := true
).dependsOn(feed, search, sort).aggregate(ring, feed, search, sort)

lazy val ring = project.in(file("ring")).settings(
  scalaVersion := scalav
, Compile / scalaSource := baseDirectory.value / "src"
, libraryDependencies ++= Seq(
    "org.apache.pekko" %% "pekko-cluster" % pekko
  , "org.rocksdb" % "rocksdbjni" % rocks
  , "dev.zio" %% "zio" % zio
  )
, scalacOptions ++= scalacOptionsCommon diff Seq("-language:strictEquality") :+ "-nowarn"
).dependsOn(proto)

lazy val sharding = project.in(file("sharding")).settings(
  scalaVersion := scalav
, Compile / scalaSource := baseDirectory.value / "src"
, libraryDependencies ++= Seq(
    "org.apache.pekko" %% "pekko-cluster-sharding" % pekko
  )
, scalacOptions ++= scalacOptionsCommon
).dependsOn(ring)

lazy val feed = project.in(file("feed")).settings(
  scalaVersion := scalav
, Compile / scalaSource := baseDirectory.value / "src"
, libraryDependencies ++= Seq(
    "dev.zio" %% "zio-streams" % zio
  )
, scalacOptions ++= scalacOptionsCommon
).dependsOn(sharding)

lazy val search = project.in(file("search")).settings(
  scalaVersion := scalav
, Compile / scalaSource := baseDirectory.value / "src"
, libraryDependencies ++= Seq(
    "dev.zio" %% "zio-streams" % zio
  , "org.apache.lucene" % "lucene-analysis-common" % lucene
  )
, scalacOptions ++= scalacOptionsCommon
).dependsOn(sharding)

lazy val sort = project.in(file("sort")).settings(
  scalaVersion := scalav
, Compile / scalaSource := baseDirectory.value / "src"
, libraryDependencies ++= Seq(
    "dev.zio" %% "zio-streams" % zio
  )
, scalacOptions ++= scalacOptionsCommon
).dependsOn(sharding)

lazy val proto = project.in(file("deps/proto/proto")).settings(
  scalaVersion := scalav
, crossScalaVersions := scalav :: Nil
, libraryDependencies ++= Seq(
    "com.google.protobuf" % "protobuf-java" % protoj
  )
, scalacOptions ++= scalacOptionsCommon diff Seq("-language:strictEquality") :+ "-Xcheck-macros"
).dependsOn(protosyntax)

lazy val protosyntax = project.in(file("deps/proto/syntax")).settings(
  scalaVersion := scalav
, crossScalaVersions := scalav :: Nil
, scalacOptions ++= scalacOptionsCommon diff Seq("-language:strictEquality")
)

val scalacOptionsCommon = Seq(
  "-Yexplicit-nulls"
, "-language:strictEquality"
)

Global / onChangedBuildSource := ReloadOnSourceChanges
