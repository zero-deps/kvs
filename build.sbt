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
  , "-Ywarn-unused:implicits"
  , "-Ywarn-unused:imports"
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

lazy val kvs = project.in(file(".")).aggregate(kvs_core, kvs_seq, kvs_search)

lazy val kvs_core = project.in(file("core"))

lazy val kvs_seq = project.in(file("seq"))
  
lazy val kvs_search = project.in(file("search"))
