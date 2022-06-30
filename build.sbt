lazy val root = project.in(file(".") ).aggregate(kvs)

lazy val kvs = project.in(file("kvs")).settings(
  scalaVersion := "3.1.1"
, libraryDependencies ++= Seq(
    "com.typesafe.akka" % "akka-cluster-sharding_2.13" % "2.6.16"
  , "com.typesafe.akka" % "akka-slf4j_2.13" % "2.6.16"
  , "ch.qos.logback" % "logback-classic" % "1.3.0-alpha5"
  , "com.github.jnr" % "jnr-ffi" % "2.2.2"
  , "org.apache.lucene" % "lucene-analyzers-common" % "8.9.0"
  , "dev.zio" %% "zio" % "2.0.0"
  , "dev.zio" %% "zio-nio" % "2.0.0"
  , "org.rocksdb" % "rocksdbjni" % "6.22.1"
  , "org.scalatest" %% "scalatest" % "3.2.11" % Test
  , "com.typesafe.akka" % "akka-testkit_2.13" % "2.6.16" % Test
  )
, scalacOptions ++= scalacOptions3
).dependsOn(proto)

lazy val proto = project.in(file("deps/proto/proto")).settings(
  scalaVersion := "3.1.1"
, crossScalaVersions := "3.1.1" :: Nil
, libraryDependencies += "com.google.protobuf" % "protobuf-java" % "3.19.3"
).dependsOn(protoops)

lazy val protoops = project.in(file("deps/proto/ops")).settings(
  scalaVersion := "3.1.1"
, crossScalaVersions := "3.1.1" :: Nil
).dependsOn(protosyntax)

lazy val protosyntax = project.in(file("deps/proto/syntax")).settings(
  scalaVersion := "3.1.1"
, crossScalaVersions := "3.1.1" :: Nil
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
