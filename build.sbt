val scalaVersion_ = "2.13.3"
val akkaVersion = "2.5.31"
val extVersion = "2.2.0.7.g8f0877e"
val leveldbVersion = "1.0.4"
val protoVersion = "1.8"
val logbackVersion = "1.2.3"
val scalatestVersion = "3.1.1"

ThisBuild / scalaVersion := scalaVersion_
ThisBuild / resolvers += Resolver.jcenterRepo
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
  , "-Ywarn-extra-implicit"
  , "-Ywarn-numeric-widen"
  , "-Ywarn-value-discard"
  , "-Ywarn-unused:implicits"
  , "-Ywarn-unused:imports"
  , "-Ywarn-unused:params"
  , "-encoding", "UTF-8"
  , "-Xmaxerrs", "1"
  , "-Xmaxwarns", "3"
  , "-Wconf:cat=deprecation&msg=Auto-application:silent"
)

ThisBuild / turbo := true
ThisBuild / useCoursier := true
Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val kvs = project.in(file("."))
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion
    , "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion
    , "com.typesafe.akka" %% "akka-slf4j"            % akkaVersion
    , "io.github.zero-deps" %% "proto-macros" % protoVersion % Compile
    , "io.github.zero-deps" %% "proto-runtime" % protoVersion
    , compilerPlugin("io.github.zero-deps" %% "ext-plug" % extVersion)
    , "io.github.zero-deps" %% "ext" % extVersion
    , "io.github.zero-deps" %% "leveldb-jnr" % leveldbVersion

    , "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
    , "org.scalatest" %% "scalatest" % scalatestVersion % Test

    , "org.apache.lucene" % "lucene-analyzers-common" % "8.4.1"
    , "org.scalatest" %% "scalatest" % scalatestVersion % Test
    )
  )
