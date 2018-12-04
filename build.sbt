val akkaVersion = "2.5.18"

lazy val root = (project in file(".")).withId("kvs")
  .settings(
    inThisBuild(
      publishSettings ++ buildSettings ++ resolverSettings ++ Seq(
        cancelable in Global := true,
        javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
        scalacOptions in Compile ++= Seq(
          "-target:jvm-1.8",
          "-feature",
          "-unchecked",
          "-deprecation",
          "-language:_",
          "-encoding", "UTF-8",
          "-Ypartial-unification",
          "-Xfatal-warnings",
        ),
      ),
    ),
    fork in Test := true,
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.github.jnr" % "jnr-ffi" % "2.1.7",
      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j"            % akkaVersion,
      "org.scalaz" %% "scalaz-core" % "7.2.27",

      "com.playtech.mws" %% "scala-pickling" % "1.0-2-gb05b7b9" % Test,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion       % Test,
      "org.scalatest" %% "scalatest" % "3.0.1"                  % Test,
    ),
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    )
  )

lazy val kvsDemo = (project in file("kvs-demo")).settings(
  mainClass in (Compile, run) := Some("mws.kvs.Run"),
  fork in run := true,
  javaOptions ++= Seq(
    "-Dcom.sun.management.jmxremote",
    "-Dcom.sun.management.jmxremote.ssl=false",
    "-Dcom.sun.management.jmxremote.authenticate=false",
    "-Dcom.sun.management.jmxremote.port=9000",
  ),
).dependsOn(root)

lazy val leveldbTest = (project in file("leveldb-test")).settings(
  testOptions += Tests.Argument(TestFrameworks.JUnit),
  libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test,
  libraryDependencies += "junit" % "junit" % "4.12" % Test,
).dependsOn(root)

lazy val buildSettings = Seq(
  organization := "com.playtech.mws",
  description := "Abstract Scala Types Key-Value Storage",
  version := org.eclipse.jgit.api.Git.open(file(".")).describe().call(),
  scalaVersion := "2.12.7"
)

lazy val resolverSettings = Seq(
  resolvers ++= Seq(
    Resolver.mavenLocal,
    "releases resolver" at "http://nexus.mobile.playtechgaming.com/nexus3/repository/releases"
  )
)

lazy val publishSettings = Seq(
  publishTo := Some("releases" at "http://nexus.mobile.playtechgaming.com/nexus3/repository/releases"),
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
  publishArtifact := true,
  publishMavenStyle := true,
  pomIncludeRepository := (_ => false),
  isSnapshot := true,
  // crossScalaVersions := Seq("2.11.12", "2.12.7")
)
