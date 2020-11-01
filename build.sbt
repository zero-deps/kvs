
val kvs = project.in(file(".")).settings(
  version := "5.0"
, scalaVersion := "2.13.3"
, resolvers += Resolver.jcenterRepo
, libraryDependencies ++= Seq(
    "org.rocksdb" % "rocksdbjni" % "6.13.3"
  , "org.lz4" % "lz4-java" % "1.7.1"
  , "org.apache.lucene" % "lucene-analyzers-common" % "8.4.1"
  , "dev.zio" %% "zio-nio" % "1.0.0-RC9"
  , "dev.zio" %% "zio-akka-cluster" % "0.2.0"
  , "dev.zio" %% "zio-macros" % "1.0.3"
  , "com.typesafe.akka" %% "akka-cluster-sharding" % "2.5.31"
  , "com.typesafe.akka" %% "akka-slf4j"            % "2.5.31"
  , "com.typesafe.akka" %% "akka-testkit"          % "2.5.31" % Test
  , "io.github.zero-deps" %% "proto-macros"  % "1.8" % Compile
  , "io.github.zero-deps" %% "proto-runtime" % "1.8"
  , compilerPlugin(
    "io.github.zero-deps" %% "ext-plug" % "2.2.0.7.g8f0877e")
  , "io.github.zero-deps" %% "ext"      % "2.2.0.7.g8f0877e"
  , "ch.qos.logback" % "logback-classic" % "1.2.3"
  , compilerPlugin(
    "org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full)
  , "org.scalatest" %% "scalatest" % "3.1.1" % Test
  )
, scalacOptions ++= opts
, turbo := true
, useCoursier := true
, Global / onChangedBuildSource := ReloadOnSourceChanges
)

val example = project.in(file("example")).dependsOn(kvs).settings(
  mainClass in (Compile, run) := Some("example.App")
, cancelable in Global := true
, fork in run := true
, scalaVersion := "2.13.3"
)

val opts = Seq(
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
