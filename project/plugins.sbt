libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.25"
libraryDependencies += "org.eclipse.jgit" % "org.eclipse.jgit" % "4.9.0.201710071750-r"
scalacOptions ++= Seq("-feature","-deprecation")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.14")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.19")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.8.2"
