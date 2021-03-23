lazy val kvs = project.in(file(".")).settings(
  version := zero.git.version()
, scalaVersion := "2.13.5"
, crossScalaVersions := "3.0.0-RC1" :: "2.13.5" :: Nil
, libraryDependencies ++= deps
, scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 13)) => opts
      case _ => Seq("-source:3.0-migration", "-language:postfixOps")
    }
  }
, resolvers += Resolver.githubPackages("zero-deps")
)

lazy val ext = project.in(file("deps/ext"))

val deps = Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3"
, "com.typesafe.akka" %% "akka-cluster-sharding" % "2.6.13" cross CrossVersion.for3Use2_13
, "com.typesafe.akka" %% "akka-slf4j"            % "2.6.13" cross CrossVersion.for3Use2_13
, "com.typesafe.akka" %% "akka-testkit"          % "2.6.13" % Test cross CrossVersion.for3Use2_13
, "io.github.zero-deps" %% "proto-macros" % "2.0.4.gdde2aab"
, "io.github.zero-deps" %% "proto-api"    % "2.0.4.gdde2aab"
// , compilerPlugin("io.github.zero-deps" %% "eq" % "2.5.2.gf1bc95b")
, "io.github.zero-deps" %% "ext" % "2.4.2.g2a97c55" cross CrossVersion.for3Use2_13
, "com.github.jnr" % "jnr-ffi" % "2.1.13"
, "org.apache.lucene" % "lucene-analyzers-common" % "8.4.1"
, "org.scalatest" %% "scalatest" % "3.2.6" % Test
)

val opts = Seq(
  "-deprecation"
, "-explaintypes"
, "-feature"
, "-language:_"
, "-unchecked"
, "-Xcheckinit"
// , "-Xfatal-warnings"
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

turbo := true
useCoursier := true
Global / onChangedBuildSource := ReloadOnSourceChanges
