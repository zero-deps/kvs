package mws.rng

import sbt.Process

object Settings {
  val organization = "com.playtech.mws"
  val description = "MWS Ring"
  lazy val version = Process("git describe").lines.head
  lazy val isRelease = Process("git tag").lines.contains(version)
  val scalaVersion = Dependencies.Versions.scalaVersion
}
