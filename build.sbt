val kvs = project.in(file(".")).settings(
  version := "5.0"
, scalaVersion := "2.13.3"
, resolvers += Resolver.jcenterRepo
, resolvers += Resolver.githubPackages("zero-deps")
, libraryDependencies ++= Seq(
    "org.rocksdb" % "rocksdbjni" % "6.13.3"
  , "org.lz4" % "lz4-java" % "1.7.1"
  , "org.apache.lucene" % "lucene-analyzers-common" % "8.4.1"
  , "dev.zio" %% "zio-nio"          % "1.0.0-RC9"
  , "dev.zio" %% "zio-macros"       % "1.0.3"
  , "dev.zio" %% "zio-test"         % "1.0.3" % Test
  , "dev.zio" %% "zio-test-sbt"     % "1.0.3" % Test
  , "com.typesafe.akka" %% "akka-cluster-sharding" % "2.5.31"
  , "com.typesafe.akka" %% "akka-slf4j"            % "2.5.31"
  , "com.typesafe.akka" %% "akka-testkit"          % "2.5.31" % Test
  , "io.github.zero-deps" %% "proto-macros"  % "1.8" % Compile
  , "io.github.zero-deps" %% "proto-runtime" % "1.8"
  , compilerPlugin(
    "io.github.zero-deps" %% "ext-plug" % "2.3.1.g6719341")
  , "io.github.zero-deps" %% "ext"      % "2.3.1.g6719341"
  , "ch.qos.logback" % "logback-classic" % "1.2.3"
  , compilerPlugin(
    "org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full)
  )
, testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
, scalacOptions ++= opts
, turbo := true
, useCoursier := true
, Global / onChangedBuildSource := ReloadOnSourceChanges
)

val examples = project.in(file("examples")).dependsOn(kvs).settings(
  cancelable in Global := true
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
  , "-Ywarn-unused:implicits"
  , "-Ywarn-unused:imports"
  , "-Ywarn-unused:params"
  , "-encoding", "UTF-8"
  , "-Xmaxerrs", "1"
  , "-Xmaxwarns", "3"
  , "-Wconf:cat=deprecation&msg=Auto-application:silent"
  , "-Ymacro-annotations"
)
