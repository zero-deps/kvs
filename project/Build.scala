package mws.rng

import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import com.typesafe.sbt.packager.archetypes._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.docker._
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import sbt.Keys._
import sbt._

object Build extends sbt.Build {

  lazy val buildSettings = Seq(
    organization := "com.playtech.mws",
    description := "MWS Ring",
    version := org.eclipse.jgit.api.Git.open(file(".")).describe().call(),
    scalaVersion := Dependencies.Versions.scala
  )

  lazy val root = Project(
    id = "rng",
    base = file("."),
    settings = Seq(
      libraryDependencies ++= Dependencies.rng,

      mainClass in Compile := Some("mws.rng.RingApp"),
      Keys.fork in run := true,

      compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
      scalacOptions in Compile ++= Seq("-encoding", "UTF-8", "-target:jvm-1.7", "-deprecation", "-feature"),
      javacOptions in compile ++= Seq("-encoding", "UTF-8", "-source", "1.7", "-target", "1.7"),

      publishTo := Some("MWS Releases" at "http://ua-mws-nexus01.ee.playtech.corp/nexus/content/repositories/releases"),
      credentials += Credentials("Sonatype Nexus Repository Manager", "ua-mws-nexus01.ee.playtech.corp", "wpl-deployer", "aG1reeshie"),
      publishArtifact := true,
      publishArtifact in Compile := true,
      publishArtifact in Test := false,
      publishMavenStyle := true,
      pomIncludeRepository := (_ => false),
      publishLocal <<= publishM2,
      isSnapshot := true,

      parallelExecution in Test := false,
      executeTests in Test <<= (executeTests in Test, executeTests in MultiJvm) map {
        case (testResults, multiNodeResults)  =>
          val overall =
            if (testResults.overall.id < multiNodeResults.overall.id)
              multiNodeResults.overall
            else
              testResults.overall
          Tests.Output(overall,
            testResults.events ++ multiNodeResults.events,
            testResults.summaries ++ multiNodeResults.summaries)
      },

      mappings in Docker <+= (defaultLinuxInstallLocation in Docker, sourceDirectory) map { (path,src) =>
        val conf = src / "main" / "resources" / "docker.conf"
        conf -> s"$path/conf/application.conf"
      },
      dockerExposedPorts in Docker := Seq(4334,9998),
      dockerEntrypoint in Docker := Seq("sh", "-c",
          "KAI_IP=`/sbin/ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{print $1}'` bin/rng $*"),
      dockerRepository := Some("playtech")
    ) ++ SbtMultiJvm.multiJvmSettings
  ).configs(MultiJvm)
   .enablePlugins(AkkaAppPackaging, DockerPlugin)

  override lazy val settings = super.settings ++
    buildSettings ++
    Seq(shellPrompt := { s => Project.extract(s).currentProject.id + " > " })
}
