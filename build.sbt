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
      "com.protonail.leveldb-jna" % "leveldb-jna-core"   % Versions.leveldb,
      "com.protonail.leveldb-jna" % "leveldb-jna-native" % Versions.leveldb classifier "osx",
      "com.protonail.leveldb-jna" % "leveldb-jna-native" % Versions.leveldb classifier "windows-x86_64",
      "com.protonail.leveldb-jna" % "leveldb-jna-native" % Versions.leveldb classifier "windows-x86",
      "com.protonail.leveldb-jna" % "leveldb-jna-native" % Versions.leveldb classifier "linux-x86_64",
      "com.protonail.leveldb-jna" % "leveldb-jna-native" % Versions.leveldb classifier "linux-x86",

      "org.scalaz"            %% "scalaz-core"             % Versions.scalaz % Provided,

      "ch.qos.logback"         % "logback-classic"         % Versions.logback % Provided,

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
      "com.playtech.mws.akka" %% "akka-testkit"            % Versions.akka      % Test
    )
  )

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
  credentials += Credentials("Sonatype Nexus Repository Manager","nexus.mobile.playtechgaming.com","wpl-deployer","aG1reeshie"),
  publishArtifact := true,
  publishMavenStyle := true,
  pomIncludeRepository := (_ => false),
  isSnapshot := true,
  crossScalaVersions := Seq("2.11.12", "2.12.6")
)
