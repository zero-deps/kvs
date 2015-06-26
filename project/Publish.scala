package mws.rng

import sbt.Keys._
import sbt._
import aether.AetherPlugin.autoImport._

object Publish {
  val settings = Seq(
    publishTo := {
      val nexus = "http://ua-mws-nexus01.ee.playtech.corp/nexus/content/repositories"
      if (isSnapshot.value)
        Some("MWS Snapshots" at s"$nexus/snapshots")
      else
        Some("MWS Releases" at s"$nexus/releases")
    },
    credentials += Credentials("Sonatype Nexus Repository Manager",
      "ua-mws-nexus01.ee.playtech.corp", "wpl-deployer", "aG1reeshie"),
    publishArtifact := true,
    publishArtifact in Compile := false,
    publishArtifact in (Compile, packageBin) := true,
    publishArtifact in Test := false,
    publishMavenStyle := true,
    pomIncludeRepository := (_ => false)
  ) ++ overridePublishBothSettings
}
