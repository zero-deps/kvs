libraryDependencies += "org.slf4j" % "slf4j-nop" % "latest.integration"
addSbtPlugin("io.github.zero-deps" % "sbt-git" % "2.5.4.g35fec15")

scalacOptions ++= Seq("-feature", "-deprecation")

ThisBuild / resolvers += Resolver.bintrayRepo("zero-deps", "maven")