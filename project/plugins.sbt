libraryDependencies += "org.slf4j" % "slf4j-nop" % "latest.integration"
libraryDependencies += "io.github.zero-deps" %% "gs-git" % "1.6.2"

scalacOptions ++= Seq("-feature", "-deprecation")

resolvers += Resolver.jcenterRepo