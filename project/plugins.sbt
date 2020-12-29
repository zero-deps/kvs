resolvers += Resolver.githubPackages("zero-deps")
addSbtPlugin("io.github.zero-deps" % "sbt-git"             % "latest.integration")
addSbtPlugin("com.codecommit"      % "sbt-github-packages" % "latest.integration")
