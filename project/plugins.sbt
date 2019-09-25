libraryDependencies += "org.slf4j" % "slf4j-nop" % "latest.integration"
libraryDependencies += "io.github.zero-deps" %% "gs-git" % "1.5.1"

scalacOptions ++= Seq("-feature", "-deprecation")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "latest.integration")

addSbtPlugin("com.github.shmishleniy" % "sbt-deploy-ssh" % "0.1.4")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.3.4")

ThisBuild / resolvers += Resolver.jcenterRepo