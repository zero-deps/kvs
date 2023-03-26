val scalav = "3.2.2"
val zio = "2.0.10"
val akka = "2.6.20"
val rocks = "7.10.2"
val protoj = "3.22.2"
val lucene = "8.11.2"

lazy val root = project.in(file(".") ).aggregate(kvs)

lazy val kvs = project.in(file("kvs")).settings(
  scalaVersion := scalav
, libraryDependencies ++= Seq(
    "com.typesafe.akka" % "akka-cluster-sharding_2.13" % akka
  , "com.typesafe.akka" % "akka-slf4j_2.13" % akka
  , "ch.qos.logback" % "logback-classic" % "1.4.5"
  , "com.github.jnr" % "jnr-ffi" % "2.2.2"
  , "org.apache.lucene" % "lucene-analyzers-common" % lucene
  , "dev.zio" %% "zio" % zio
  , "dev.zio" %% "zio-nio" % "2.0.0"
  , "org.rocksdb" % "rocksdbjni" % rocks
  , "org.scalatest" %% "scalatest" % "3.2.14" % Test
  , "com.typesafe.akka" % "akka-testkit_2.13" % akka % Test
  )
, scalacOptions ++= scalacOptions3
).dependsOn(proto)

lazy val proto = project.in(file("deps/proto/proto")).settings(
  scalaVersion := scalav
, crossScalaVersions := scalav :: Nil
, libraryDependencies += "com.google.protobuf" % "protobuf-java" % protoj
).dependsOn(protoops)

lazy val protoops = project.in(file("deps/proto/ops")).settings(
  scalaVersion := scalav
, crossScalaVersions := scalav :: Nil
).dependsOn(protosyntax)

lazy val protosyntax = project.in(file("deps/proto/syntax")).settings(
  scalaVersion := scalav
, crossScalaVersions := scalav :: Nil
)

val scalacOptions3 = Seq(
  "-source:future", "-nowarn"
, "-language:strictEquality"
, "-language:postfixOps"
, "-Yexplicit-nulls"
, "-encoding", "UTF-8"
)

turbo := true
useCoursier := true
Global / onChangedBuildSource := ReloadOnSourceChanges
