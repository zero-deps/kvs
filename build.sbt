lazy val root = project.in(file(".")).settings(
  scalaVersion := "3.0.0-RC2"
, crossScalaVersions := "3.0.0-RC2" :: "2.13.5" :: Nil
).aggregate(kvs)

lazy val kvs = project.in(file("kvs")).settings(
  scalaVersion := "3.0.0-RC2"
, crossScalaVersions := "3.0.0-RC2" :: "2.13.5" :: Nil
, libraryDependencies ++= Seq(
    "com.typesafe.akka" % "akka-cluster-sharding_2.13" % "2.6.14"
  , "com.typesafe.akka" % "akka-slf4j_2.13" % "2.6.14"
  , "ch.qos.logback" % "logback-classic" % "1.3.0-alpha5"
  , "com.github.jnr" % "jnr-ffi" % "2.2.2"
  , "org.apache.lucene" % "lucene-analyzers-common" % "8.8.1"
  , "org.scalatest" %% "scalatest" % "3.2.7" % Test
  )
, scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 13)) => scalacOptions2
      case _ => scalacOptions3
    }
  }
).dependsOn(proto)

lazy val proto = project.in(file("deps/proto/proto")).settings(
  scalaVersion := "3.0.0-RC2"
, crossScalaVersions := "3.0.0-RC2" :: "2.13.5" :: Nil
, libraryDependencies += "com.google.protobuf" % "protobuf-java" % "3.15.7"
).dependsOn(protoops)

lazy val protoops = project.in(file("deps/proto/ops")).settings(
  scalaVersion := "3.0.0-RC2"
, crossScalaVersions := "3.0.0-RC2" :: "2.13.5" :: Nil
, libraryDependencies ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 13)) => Seq("org.scala-lang" % "scala-reflect" % scalaVersion.value)
      case _ => Nil
    }
  }
).dependsOn(protosyntax)

lazy val protosyntax = project.in(file("deps/proto/syntax")).settings(
  scalaVersion := "3.0.0-RC2"
, crossScalaVersions := "3.0.0-RC2" :: "2.13.5" :: Nil
)

val scalacOptions2 = Seq(
  "-feature", "-language:_", "-unchecked", "-nowarn"
, "-encoding", "UTF-8"
)
val scalacOptions3 = Seq(
  "-source", "future-migration", "-deprecation", "-nowarn"
, "-language:strictEquality", "-language:postfixOps"
, "-Yexplicit-nulls"
, "-encoding", "UTF-8"
, "-release", "11"
)

turbo := true
useCoursier := true
Global / onChangedBuildSource := ReloadOnSourceChanges
