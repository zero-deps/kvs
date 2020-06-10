val scalaVersion_ = "2.13.2"
val akkaVersion = "2.5.31"
val extVersion = "2.1.1"
val leveldbVersion = "1.0.4"
val protoVersion = "1.7.1-2-gf29fcc7"
val logbackVersion = "1.2.3"
val scalatestVersion = "3.1.1"

ThisBuild / organization := "io.github.zero-deps"
ThisBuild / description := "Abstract Scala Types Key-Value Storage"
ThisBuild / licenses := "MIT" -> url("https://raw.githubusercontent.com/zero-deps/kvs/master/LICENSE") :: Nil
ThisBuild / version := zero.ext.git.version
ThisBuild / scalaVersion := scalaVersion_
ThisBuild / resolvers += Resolver.jcenterRepo
ThisBuild / resolvers += Resolver.bintrayRepo("zero-deps", "maven")
ThisBuild / cancelable in Global := true
ThisBuild / javacOptions ++= Seq("-source", "13", "-target", "13")
ThisBuild / scalacOptions ++= Seq(
    "-deprecation"
  , "-explaintypes"
  , "-feature"
  , "-language:_"
  , "-unchecked"
  , "-Xcheckinit"
  , "-Xfatal-warnings"
  , "-Xlint:adapted-args"
  , "-Xlint:constant"
  , "-Xlint:delayedinit-select"
  , "-Xlint:inaccessible"
  , "-Xlint:infer-any"
  , "-Xlint:missing-interpolator"
  , "-Xlint:nullary-override"
  , "-Xlint:nullary-unit"
  , "-Xlint:option-implicit"
  , "-Xlint:package-object-classes"
  , "-Xlint:poly-implicit-overload"
  , "-Xlint:private-shadow"
  , "-Xlint:stars-align"
  , "-Xlint:type-parameter-shadow"
  , "-Ywarn-dead-code"
  , "-Ywarn-extra-implicit"
  , "-Ywarn-numeric-widen"
  , "-Ywarn-value-discard"
  , "-Ywarn-unused:implicits"
  , "-Ywarn-unused:imports"
  , "-Ywarn-unused:params"
  , "-target:jvm-12"
  , "-encoding", "UTF-8"
)

ThisBuild / isSnapshot := true // override local artifacts

ThisBuild / turbo := true
ThisBuild / useCoursier := true
Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val root = project.in(file(".")).aggregate(core, search, demo)
  .settings(
    name := s"kvs-${name.value}"
  , skip in publish := true
  )

lazy val core = project.in(file("core"))
  .settings(
    scalacOptions in Test := Nil,
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j"            % akkaVersion,
      "io.github.zero-deps" %% "proto-macros" % protoVersion % Compile,
      "io.github.zero-deps" %% "proto-runtime" % protoVersion,
      compilerPlugin("io.github.zero-deps" %% "ext-plug" % extVersion),
      "io.github.zero-deps" %% "ext" % extVersion,
      "io.github.zero-deps" %% "leveldb-jnr" % leveldbVersion,

      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
    )
  , name := s"kvs-${name.value}"
  , publishArtifact := true
  )
  
lazy val search = project.in(file("search"))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.lucene" % "lucene-analyzers-common" % "8.4.1"
    , compilerPlugin("io.github.zero-deps" %% "ext-plug" % extVersion)
    , "org.scalatest" %% "scalatest" % scalatestVersion % Test
    )
  , name := s"kvs-${name.value}"
  , publishArtifact := true
  )
  .dependsOn(core)

lazy val demo = project.in(file("demo"))
  .settings(
    mainClass in (Compile, run) := Some("zd.kvs.Run")
  , fork in run := true
  , skip in publish := true
  )
  .dependsOn(core)
