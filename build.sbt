val scalav = "3.8.4"
val zio = "2.1.26"
val akka = "2.6.20"
val rocks = "7.10.2"
val protoj = "3.22.2"
val lucene = "8.11.2"

lazy val root = project.in(file(".")).settings(
  scalaVersion := scalav
, scalacOptions ++= scalacOptions3
).dependsOn(kvs).aggregate(kvs)

lazy val kvs = project.in(file("kvs")).settings(
  scalaVersion := scalav
, libraryDependencies ++= Seq(
    ("com.typesafe.akka" %% "akka-cluster-sharding" % akka).cross(CrossVersion.for3Use2_13)
  , ("com.typesafe.akka" %% "akka-slf4j" % akka).cross(CrossVersion.for3Use2_13)
  , "ch.qos.logback" % "logback-classic" % "1.4.5"
  , "com.github.jnr" % "jnr-ffi" % "2.2.18"
  , "org.apache.lucene" % "lucene-analyzers-common" % lucene
  , "dev.zio" %% "zio" % zio
  , "dev.zio" %% "zio-nio" % "2.0.2"
  , "org.rocksdb" % "rocksdbjni" % rocks
  , "org.scalatest" %% "scalatest" % "3.2.14" % Test
  , ("com.typesafe.akka" %% "akka-testkit" % akka % Test).cross(CrossVersion.for3Use2_13)
  )
, scalacOptions ++= scalacOptions3
).dependsOn(proto)

lazy val proto = project.in(file("deps/proto/proto")).settings(
  scalaVersion := scalav
, crossScalaVersions := scalav :: Nil
, libraryDependencies += "com.google.protobuf" % "protobuf-java" % protoj
).dependsOn(protosyntax)

lazy val protosyntax = project.in(file("deps/proto/syntax")).settings(
  scalaVersion := scalav
, crossScalaVersions := scalav :: Nil
)

val scalacOptions3 = Seq(
  "-nowarn", "-language:strictEquality"
, "-language:postfixOps"
, "-Yexplicit-nulls"
, "-encoding", "UTF-8"
)

Global / onChangedBuildSource := ReloadOnSourceChanges
