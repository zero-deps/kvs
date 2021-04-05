lazy val kvs = project.in(file(".")).settings(
  version := zero.git.version()
, scalaVersion := "2.13.5"
, libraryDependencies ++= Seq(
    "org.rocksdb" % "rocksdbjni" % "6.14.6"
  , "org.lz4" % "lz4-java" % "1.7.1"
  , "org.apache.lucene" % "lucene-analyzers-common" % "8.4.1"
  , "dev.zio" %% "zio-nio" % "1.0.0-RC9"
  , "dev.zio" %% "zio-macros"   % "1.0.5"
  , "dev.zio" %% "zio-test-sbt" % "1.0.5" % Test
  , "com.typesafe.akka" %% "akka-cluster-sharding" % "2.6.13"
  , compilerPlugin("org.typelevel" %% "kind-projector" % "0.11.3" cross CrossVersion.full)
  )
, testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
, scalacOptions ++= Seq(
    "-encoding", "UTF-8"
  , "-language:postfixOps"
  , "-Wconf:cat=deprecation&msg=Auto-application:silent"
  , "-Ymacro-annotations"
  )
).dependsOn(proto)

lazy val proto = project.in(file("deps/proto/proto")).settings(
  scalaVersion := "2.13.5"
, crossScalaVersions := "2.13.5" :: Nil
, libraryDependencies += "com.google.protobuf" % "protobuf-java" % "3.15.6"
).dependsOn(protoops)

lazy val protoops = project.in(file("deps/proto/ops")).settings(
  scalaVersion := "2.13.5"
, crossScalaVersions := "2.13.5" :: Nil
, libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
).dependsOn(protosyntax)

lazy val protosyntax = project.in(file("deps/proto/syntax")).settings(
  scalaVersion := "2.13.5"
, crossScalaVersions := "2.13.5" :: Nil
)

lazy val examples = project.in(file("examples")).dependsOn(kvs).settings(
  fork := true
, scalaVersion := "2.13.5"
)

turbo := true
useCoursier := true
Global / onChangedBuildSource := ReloadOnSourceChanges
Global / cancelable := true
