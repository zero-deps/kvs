lazy val kvs = project.in(file(".")).settings(
  libraryDependencies ++= deps
, resolvers += Resolver.jcenterRepo
, resolvers += Resolver.githubPackages("zero-deps")
, scalacOptions ++= opts
, scalaVersion := "2.13.3"
, turbo := true
, useCoursier := true
, Global / onChangedBuildSource := ReloadOnSourceChanges
)

val deps = Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3"
, "com.typesafe.akka" %% "akka-cluster-sharding" % "2.5.31"
, "com.typesafe.akka" %% "akka-slf4j"            % "2.5.31"
, "com.typesafe.akka" %% "akka-testkit"          % "2.5.31" % Test
, "io.github.zero-deps" %% "proto-macros"  % "1.8" % Compile
, "io.github.zero-deps" %% "proto-runtime" % "1.8"
, compilerPlugin(
  "io.github.zero-deps" %% "ext-plug" % "2.2.0.7.g8f0877e")
, "io.github.zero-deps" %% "ext"      % "2.2.0.7.g8f0877e"
, "com.github.jnr" % "jnr-ffi" % "2.1.13"
, "org.apache.lucene" % "lucene-analyzers-common" % "8.4.1"
, "org.scalatest" %% "scalatest" % "3.1.1" % Test
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
