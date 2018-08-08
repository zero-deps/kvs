lazy val root = (project in file(".")).withId("kvs")
  .settings(
    inThisBuild(
      publishSettings ++ buildSettings ++ resolverSettings
    ),
    shellPrompt := (Project.extract(_).currentProject.id + " > "),
    mainClass in (Compile,run) := Some("mws.kvs.Run"),
    cancelable in Global := true,
    fork in run := true,
    scalacOptions in Compile ++= Seq(
      "-feature",
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-encoding", "UTF-8",
      "-Ypartial-unification",
      "-Xfatal-warnings",
      "-Ywarn-dead-code",
      "-Ywarn-unused-import"
    ),
    fork in Test := true,
    libraryDependencies ++= Seq(
      "com.github.jnr" % "jnr-ffi" % "2.1.7",

      "org.scalaz" %% "scalaz-core" % Versions.scalaz,

      "ch.qos.logback" % "logback-classic" % Versions.logback,

      "com.playtech.mws.akka" %% "akka-actor"              % Versions.akka,
      "com.playtech.mws.akka" %% "akka-cluster"            % Versions.akka,
      "com.playtech.mws.akka" %% "akka-cluster-sharding"   % Versions.akka,
      "com.playtech.mws.akka" %% "akka-cluster-tools"      % Versions.akka,
      "com.playtech.mws.akka" %% "akka-distributed-data"   % Versions.akka,
      "com.playtech.mws.akka" %% "akka-protobuf"           % Versions.akka,
      "com.playtech.mws.akka" %% "akka-remote"             % Versions.akka,
      "com.playtech.mws.akka" %% "akka-slf4j"              % Versions.akka,
      "com.playtech.mws.akka" %% "akka-stream"             % Versions.akka,

      "com.playtech.mws"      %% "scala-pickling"          % Versions.pickling  % Test,
      "org.scalatest"         %% "scalatest"               % Versions.scalatest % Test,
      "com.playtech.mws.akka" %% "akka-multi-node-testkit" % Versions.akka      % Test,
      "com.playtech.mws.akka" %% "akka-stream-testkit"     % Versions.akka      % Test,
      "com.playtech.mws.akka" %% "akka-testkit"            % Versions.akka      % Test,
    )
  )

lazy val leveldbTest = (project in file("leveldb-test")).settings(
  testOptions += Tests.Argument(TestFrameworks.JUnit),
  libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test,
  libraryDependencies += "junit" % "junit" % "4.12" % Test,
).dependsOn(root)

lazy val buildSettings = Seq(
  organization := "com.playtech.mws",
  description := "Abstract Scala Types Key-Value Storage",
  version := org.eclipse.jgit.api.Git.open(file(".")).describe().call(),
  scalaVersion := Versions.scala
)

lazy val resolverSettings = Seq(
  resolvers ++= Seq(
    Resolver.mavenLocal,
    "releases resolver" at "http://nexus.mobile.playtechgaming.com/nexus/content/repositories/releases"
  )
)

lazy val publishSettings = Seq(
  publishTo := Some("releases" at "http://nexus.mobile.playtechgaming.com/nexus/content/repositories/releases"),
  credentials += Credentials("Sonatype Nexus Repository Manager", "nexus.mobile.playtechgaming.com", "wpl-deployer", "aG1reeshie"),
  publishArtifact := true,
  publishMavenStyle := true,
  pomIncludeRepository := (_ => false),
  isSnapshot := true,
  crossScalaVersions := Seq("2.11.12", "2.12.6")
)
