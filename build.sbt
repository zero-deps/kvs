val scalaVersion_ = "2.12.8"
val scalazVersion = "7.2.27"
val akkaVersion = "2.5.18"

lazy val root = (project in file(".")).withId("kvs")
  .settings(
    inThisBuild(
      publishSettings ++ Seq(
        organization := "com.playtech.mws",
        description := "Abstract Scala Types Key-Value Storage",
        version := org.eclipse.jgit.api.Git.open(file(".")).describe().call(),
        scalaVersion := scalaVersion_,
        resolvers += "releases" at "http://nexus.mobile.playtechgaming.com/nexus3/repository/releases",
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
          "-Ywarn-unused-import",
        ),
      ),
    ),
    fork in Test := true,
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.github.jnr" % "jnr-ffi" % "2.1.7",
      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j"            % akkaVersion,
      "org.scalaz" %% "scalaz-core" % scalazVersion,

      "com.playtech.mws" %% "scala-pickling" % "1.0-2-gb05b7b9" % Test,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion       % Test,
      "org.scalatest" %% "scalatest" % "3.0.1"                  % Test,
    ),
    libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ),
  )

import deployssh.DeploySSH.{ServerConfig, ArtifactSSH}
import fr.janalyse.ssh._

lazy val demo = (project in file("kvs-demo")).settings(
  mainClass in (Compile, run) := Some("mws.kvs.Run"),
  fork in run := true,
  // javaOptions ++= Seq(
  //   "-Dcom.sun.management.jmxremote",
  //   "-Dcom.sun.management.jmxremote.ssl=false",
  //   "-Dcom.sun.management.jmxremote.authenticate=false",
  //   "-Dcom.sun.management.jmxremote.port=9000",
  // ),
  javaOptions in Universal ++= Seq(
    "-J-XX:+PreserveFramePointer"
  ),
  deployConfigs ++= Seq(
    ServerConfig(name="cms1", host="ua-mws-newcms1.ee.playtech.corp", user=Some("anle")),
    ServerConfig(name="cms2", host="ua-mws-newcms2.ee.playtech.corp", user=Some("anle")),
    ServerConfig(name="cms3", host="ua-mws-newcms3.ee.playtech.corp", user=Some("anle")),
  ),
  deployArtifacts ++= Seq(
    ArtifactSSH((packageBin in Universal).value, s"perf_data")
  ),
  deploySshExecBefore ++= Seq(
    (ssh: SSH) => ssh.shell{ shell =>
      shell.execute("cd perf_data")
      shell.execute("touch pid")
      val pid = shell.execute("cat pid")
      shell.execute(s"kill -9 ${pid}")
      shell.execute("rm -v -rf kvsdemo*")
      shell.execute("rm pid")
    }
  ),
  deploySshExecAfter ++= Seq(
    (ssh: SSH) => {
      ssh.scp { scp =>
        scp.send(file(s"./kvs-demo/src/main/resources/${ssh.options.name.get}.conf"), s"perf_data/app.conf")
      }
      ssh.shell{ shell =>
        val name = (packageName in Universal).value
        val script = (executableScriptName in Universal).value
        shell.execute("cd perf_data")
        shell.execute(s"unzip -q -o ${name}.zip")
        shell.execute(s"nohup ./${name}/bin/${script} -Dconfig.file=/home/anle/perf_data/app.conf &")
        // shell.execute(s"nohup ./${name}/bin/${script} &")
        shell.execute("echo $! > pid")
        shell.execute("touch pid")
        val pid = shell.execute("cat pid")
        val (_, status) = shell.executeWithStatus("echo $?")
        if (status != 0 || pid == "") {
         throw new RuntimeException(s"status=${status}, pid=${pid}. please check package")
        }
      }
    }
  )
).dependsOn(root).enablePlugins(JavaAppPackaging, DeploySSH, JmhPlugin)

lazy val leveldbTest = (project in file("leveldb-test")).settings(
  testOptions += Tests.Argument(TestFrameworks.JUnit),
  libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test,
  libraryDependencies += "junit" % "junit" % "4.12" % Test,
).dependsOn(root)

lazy val publishSettings = Seq(
  publishTo := Some("releases" at "http://nexus.mobile.playtechgaming.com/nexus3/repository/releases"),
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
  publishArtifact := true,
  publishMavenStyle := true,
  pomIncludeRepository := (_ => false),
  isSnapshot := true,
  // crossScalaVersions := Seq("2.11.12", scalaVersion)
)
