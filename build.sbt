val scalaVersion_ = "2.12.9"
val scalazVersion = "7.2.27"
val akkaVersion = "2.5.23"

ThisBuild / organization := "io.github.zero-deps"
ThisBuild / description := "Abstract Scala Types Key-Value Storage"
ThisBuild / licenses := "MIT" -> url("https://raw.githubusercontent.com/zero-deps/kvs/master/LICENSE") :: Nil
ThisBuild / version := {
  val repo = org.eclipse.jgit.api.Git.open(file("."))
  val desc = repo.describe.call
  val dirty = if (repo.status.call.isClean) "" else "-dirty"
  s"${desc}${dirty}"
}
ThisBuild / scalaVersion := scalaVersion_
ThisBuild / resolvers += Resolver.jcenterRepo
ThisBuild / cancelable in Global := true
ThisBuild / javacOptions ++= Seq("-source", "12", "-target", "12")
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
ThisBuild / isSnapshot := true // override local artifacts

lazy val kvs = project.in(file("."))
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j"            % akkaVersion,
      "org.scalaz" %% "scalaz-core" % scalazVersion,
      "io.github.zero-deps" %% "proto-macros" % "1.2.2" % Compile,
      "io.github.zero-deps" %% "proto-runtime" % "1.2.2",
      compilerPlugin("io.github.zero-deps" %% "gs-plug" % "1.0.0"),
      "io.github.zero-deps" %% "leveldb-jnr" % "1.0.0",
      "io.github.zero-deps" %% "leveldb-jnr" % "1.0.0" % Test classifier "tests",

      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "org.scalatest" %% "scalatest" % "3.0.1" % Test,
    )
  )

import deployssh.DeploySSH.{ServerConfig, ArtifactSSH}
import fr.janalyse.ssh._

lazy val demo = (project in file("kvs-demo")).settings(
  mainClass in (Compile, run) := Some("zd.kvs.Run"),
  fork in run := true,
  javaOptions in Universal ++= Seq(
    "-J-XX:+PreserveFramePointer"
  ),
  deployConfigs ++= Seq(
    ServerConfig(name="env1", host="host1", user=Some("user1")),
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
