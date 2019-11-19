val scalaVersion_ = "2.13.1"
val akkaVersion = "2.5.26"
val gsVersion = "1.6.2"
val leveldbVersion = "1.0.3"
val protoVersion = "1.7.0"
val logbackVersion = "1.2.3"
val scalatestVersion = "3.1.0-SNAP13"

ThisBuild / organization := "io.github.zero-deps"
ThisBuild / description := "Abstract Scala Types Key-Value Storage"
ThisBuild / licenses := "MIT" -> url("https://raw.githubusercontent.com/zero-deps/kvs/master/LICENSE") :: Nil
ThisBuild / version := zd.gs.git.GitOps.version
ThisBuild / scalaVersion := scalaVersion_
ThisBuild / resolvers += Resolver.jcenterRepo
ThisBuild / cancelable in Global := true
ThisBuild / javacOptions ++= Seq("-source", "13", "-target", "13")
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
  , "-Xlint:nullary-override"
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
  , "-target:jvm-12"
  , "-encoding", "UTF-8"
)

ThisBuild / isSnapshot := true // override local artifacts
ThisBuild / publishArtifact := true
ThisBuild / publishArtifact in Test := true

ThisBuild / turbo := true
ThisBuild / useCoursier := true
Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val kvs = project.in(file("."))
  .settings(
    scalacOptions in Test := Nil,
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j"            % akkaVersion,
      "io.github.zero-deps" %% "proto-macros" % protoVersion % Compile,
      "io.github.zero-deps" %% "proto-runtime" % protoVersion,
      compilerPlugin("io.github.zero-deps" %% "gs-plug" % gsVersion),
      "io.github.zero-deps" %% "gs-z" % gsVersion,
      "io.github.zero-deps" %% "leveldb-jnr" % leveldbVersion,
      "io.github.zero-deps" %% "leveldb-jnr" % leveldbVersion % Test classifier "tests",

      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
    )
  )

import deployssh.DeploySSH.{ServerConfig, ArtifactSSH}
import fr.janalyse.ssh._

lazy val demo = (project in file("demo")).settings(
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
