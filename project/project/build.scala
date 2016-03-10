package kvs.project

import sbt._
import sbt.Keys._

object Build extends sbt.Build {
  override lazy val settings = super.settings ++ Seq(
    libraryDependencies += ("org.eclipse.jgit" % "org.eclipse.jgit.pgm" % "4.2.0.201601211800-r").
      exclude("org.slf4j","slf4j-api").
      exclude("org.slf4j","slf4j-log4j12").
      exclude("log4j","log4j"),
    libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.18",
    scalacOptions ++= Seq("-feature", "-deprecation")
  )
}
