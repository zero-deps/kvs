libraryDependencies += "org.slf4j" % "slf4j-nop" % "latest.integration"
libraryDependencies += "io.github.zero-deps" %% "ext-git" % "2.2.0"

scalacOptions ++= Seq("-feature", "-deprecation")

ThisBuild / resolvers += Resolver.bintrayRepo("zero-deps", "maven")