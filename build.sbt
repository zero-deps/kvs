val scalaVersion_ = "2.12.8"
val scalazVersion = "7.2.27"
val akkaVersion = "2.5.19"

ThisBuild / organization := "com.playtech.mws"
ThisBuild / description := "Abstract Scala Types Key-Value Storage"
ThisBuild / version := {
  val repo = org.eclipse.jgit.api.Git.open(file("."))
  val desc = repo.describe.call
  val dirty = if (repo.status.call.isClean) "" else "-dirty"
  s"${desc}${dirty}"
}
ThisBuild / scalaVersion := scalaVersion_
ThisBuild / resolvers += "releases" at "http://nexus.mobile.playtechgaming.com/nexus3/repository/releases"
ThisBuild / resolvers += "jcenter-proxy" at "http://nexus.mobile.playtechgaming.com/nexus3/repository/jcenter-proxy"
ThisBuild / cancelable in Global := true
ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
ThisBuild / scalacOptions in Compile ++= Seq(
  "-target:jvm-1.8",
  "-feature",
  "-unchecked",
  "-deprecation",
  "-language:_",
  "-encoding", "UTF-8",
  "-Ypartial-unification",
  "-Xfatal-warnings",
  "-Ywarn-unused-import",
)
ThisBuild / publishTo := Some("releases" at "http://nexus.mobile.playtechgaming.com/nexus3/repository/releases")
ThisBuild / credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
ThisBuild / publishArtifact := true
ThisBuild / publishMavenStyle := true
ThisBuild / pomIncludeRepository := (_ => false)
ThisBuild / isSnapshot := true

lazy val kvs = project.in(file("."))
  .settings(
    fork in Test := true,
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.github.jnr" % "jnr-ffi" % "2.1.7",
      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j"            % akkaVersion,
      "org.scalaz" %% "scalaz-core" % scalazVersion,
      "io.github.zero-deps" %% "proto-macros" % "1.1.3" % Compile,
      "io.github.zero-deps" %% "proto-runtime" % "1.1.3",
      compilerPlugin("io.github.zero-deps" %% "gs-plug" % "0-10-g6f2f17b"),

      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "org.scalatest" %% "scalatest" % "3.0.1" % Test,
    )
  )

import deployssh.DeploySSH.{ServerConfig, ArtifactSSH}
import fr.janalyse.ssh._

lazy val demo = (project in file("kvs-demo")).settings(
  mainClass in (Compile, run) := Some("mws.kvs.Run"),
  fork in run := true,
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
).dependsOn(kvs).enablePlugins(JavaAppPackaging, DeploySSH, JmhPlugin)

lazy val leveldbTest = (project in file("leveldb-test")).settings(
  testOptions += Tests.Argument(TestFrameworks.JUnit),
  libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test,
  libraryDependencies += "junit" % "junit" % "4.12" % Test,
).dependsOn(kvs)
