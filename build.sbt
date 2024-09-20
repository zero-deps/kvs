val scalav = "3.3.3"
val zio = "2.1.9"
val pekko = "1.1.1"
val rocks = "9.6.1"
val protoj = "4.28.2"
val lucene = "9.11.1"

lazy val `kvs-root` = project.in(file(".")).settings(
  scalaVersion := scalav
, libraryDependencies ++= Seq(
    "dev.zio" %% "zio-test-sbt" % zio % Test
  , "org.apache.pekko" %% "pekko-cluster-sharding" % pekko
  )
, scalacOptions ++= Seq(
    "-language:strictEquality"
  , "-Wunused:imports"
  , "-Xfatal-warnings"
  , "-Yexplicit-nulls"
  )
, run / fork := true
, run / javaOptions += "--add-modules=jdk.incubator.vector"
, run / connectInput := true
).dependsOn(kvs).aggregate(kvs)

lazy val kvs = project.in(file("kvs")).settings(
  scalaVersion := scalav
, libraryDependencies ++= Seq(
    "dev.zio" %% "zio-streams" % zio
  , "dev.zio" %% "zio-test-sbt" % zio % Test
  , "org.apache.lucene" % "lucene-analysis-common" % lucene
  , "org.apache.pekko" %% "pekko-cluster-sharding" % pekko
  , "org.rocksdb" % "rocksdbjni" % rocks
  )
, testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
, Test / fork := true
, scalacOptions ++= Seq(
    "-language:strictEquality"
  , "-Wunused:imports"
  , "-Xfatal-warnings"
  , "-Yexplicit-nulls"
  )
).dependsOn(proto)

lazy val proto = project.in(file("deps/proto/proto")).settings(
  scalaVersion := scalav
, libraryDependencies += "com.google.protobuf" % "protobuf-java" % protoj
).dependsOn(`proto-syntax`)

lazy val `proto-syntax` = project.in(file("deps/proto/syntax")).settings(
  scalaVersion := scalav
)

Global / onChangedBuildSource := ReloadOnSourceChanges
