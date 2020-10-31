ThisBuild / version := "5.0"
ThisBuild / scalaVersion := "2.13.3"
ThisBuild / cancelable in Global := true
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
  // , "-Ywarn-unused:implicits"
  // , "-Ywarn-unused:imports"
  , "-Ywarn-unused:params"
  , "-encoding", "UTF-8"
  , "-Xmaxerrs", "1"
  , "-Xmaxwarns", "1"
  , "-Wconf:cat=deprecation&msg=Auto-application:silent"
  , "-Ymacro-annotations"
)
ThisBuild / Test / scalacOptions += "-deprecation"

ThisBuild / resolvers += Resolver.jcenterRepo

ThisBuild / turbo := true
ThisBuild / useCoursier := true
Global / onChangedBuildSource := ReloadOnSourceChanges

val akka = "2.5.31"
val ext = "2.2.0.7.g8f0877e"
val logback = "1.2.3"
val lucene = "8.4.1"
val proto = "1.8"
val rocksdb = "6.13.3"
val scalatest = "3.1.1"
val zio = "1.0.3"
val zioakka = "0.2.0"
val zionio = "1.0.0-RC9"

lazy val kvs = project.in(file(".")).settings(
  libraryDependencies ++= Seq(
    "org.rocksdb" % "rocksdbjni" % rocksdb
  , "ch.qos.logback" % "logback-classic" % logback
  , "com.typesafe.akka" %% "akka-cluster-sharding" % akka
  , "com.typesafe.akka" %% "akka-slf4j"            % akka
  , "io.github.zero-deps" %% "proto-macros"  % proto % Compile
  , "io.github.zero-deps" %% "proto-runtime" % proto
  , "io.github.zero-deps" %% "ext" % ext
  , compilerPlugin("io.github.zero-deps" %% "ext-plug" % ext)

  , "dev.zio" %% "zio-nio" % zionio
  , "dev.zio" %% "zio-akka-cluster" % zioakka
  , "dev.zio" %% "zio-macros" % zio
  , compilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full)
  , "org.apache.lucene" % "lucene-analyzers-common" % lucene

  , "com.typesafe.akka" %% "akka-testkit" % akka % Test
  , "org.scalatest" %% "scalatest" % scalatest % Test
  )
)

lazy val example = project.in(file("example")).dependsOn(kvs).settings(
  mainClass in (Compile, run) := Some("example.App"),
  fork in run := true,
)
