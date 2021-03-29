lazy val kvs = project.in(file(".")).settings(
  version := zero.git.version()
, scalaVersion := "3.0.0-RC1"
, crossScalaVersions := "3.0.0-RC1" :: "2.13.5" :: Nil
, libraryDependencies ++= deps
, scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 13)) => opts
      case _ => Seq(
        "-source", "future-migration", "-deprecation", "-rewrite"
      , "-language:strictEquality", "-language:postfixOps"
      , "-Yexplicit-nulls"
      , "release", "11"
      )
    }
  }
).dependsOn(proto, ext)

lazy val proto = project.in(file("deps/proto/proto")).settings(
  scalaVersion := "3.0.0-RC1"
, crossScalaVersions := "3.0.0-RC1" :: "2.13.5" :: Nil
, libraryDependencies += "com.google.protobuf" % "protobuf-java" % "3.15.6"
).dependsOn(protoops)

lazy val protoops = project.in(file("deps/proto/ops")).settings(
  scalaVersion := "3.0.0-RC1"
, crossScalaVersions := "3.0.0-RC1" :: "2.13.5" :: Nil
, libraryDependencies ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 13)) => Seq("org.scala-lang" % "scala-reflect" % scalaVersion.value)
      case _ => Nil
    }
  }
).dependsOn(protosyntax, ext)

lazy val protosyntax = project.in(file("deps/proto/syntax")).settings(
  scalaVersion := "3.0.0-RC1"
, crossScalaVersions := "3.0.0-RC1" :: "2.13.5" :: Nil
)

lazy val ext = project.in(file("deps/proto/deps/ext")).settings(
  scalaVersion := "3.0.0-RC1"
, crossScalaVersions := "3.0.0-RC1" :: "2.13.5" :: Nil
)

val akka = "cluster-sharding" :: "slf4j" :: "actor-typed" :: "cluster-sharding-typed" :: Nil

val deps = Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3"
, "com.github.jnr" % "jnr-ffi" % "2.1.13"
, "org.apache.lucene" % "lucene-analyzers-common" % "8.4.1"
, "org.scalatest" %% "scalatest" % "3.2.6" % Test
) ++ akka.map(a => "com.typesafe.akka" %% s"akka-$a" % "2.6.13" cross CrossVersion.for3Use2_13)

val opts = Seq(
  "-feature", "-language:_", "-unchecked"
, "-encoding", "UTF-8"
, "-Wconf:cat=deprecation&msg=Auto-application:silent"
)

turbo := true
useCoursier := true
Global / onChangedBuildSource := ReloadOnSourceChanges
