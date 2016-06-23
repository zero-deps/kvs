package mws.rng

import sbt.Keys._
import sbt._

object Publish {
  val settings = Seq(
    publishTo := Some("MWS Releases" at "http://ua-mws-nexus01.ee.playtech.corp/nexus/content/repositories/releases"),
    credentials += Credentials("Sonatype Nexus Repository Manager", "ua-mws-nexus01.ee.playtech.corp", "wpl-deployer", "aG1reeshie"),
    publishArtifact := true,
    publishArtifact in Compile := true,
    publishArtifact in Test := false,
    publishMavenStyle := true,
    pomIncludeRepository := (_ => false),
    publishLocal <<= publishM2,
    isSnapshot := true
  )
}
