val akka = "2.5.31"
val ext = "2.2.0.7.g8f0877e"
val leveldb = "1.0.4"
val proto = "1.8"
val logback = "1.2.3"
val scalatest = "3.1.1"
val zionio = "1.0.0-RC9"
val zioakka = "0.2.0"

lazy val kvs_core = project.in(file("../core")).settings(
  libraryDependencies ++= Seq(
    "ch.qos.logback" % "logback-classic" % logback,
    "com.typesafe.akka" %% "akka-cluster-sharding" % akka,
    "com.typesafe.akka" %% "akka-slf4j"            % akka,
    "io.github.zero-deps" %% "proto-macros" % proto % Compile,
    "io.github.zero-deps" %% "proto-runtime" % proto,
    compilerPlugin("io.github.zero-deps" %% "ext-plug" % ext),
    "io.github.zero-deps" %% "ext" % ext,
    "io.github.zero-deps" %% "leveldb-jnr" % leveldb,

    "com.typesafe.akka" %% "akka-testkit" % akka % Test,
    "org.scalatest" %% "scalatest" % scalatest % Test,
  )
)

lazy val kvs_sec = project.in(file(".")).settings(
  libraryDependencies ++= Seq(
    "dev.zio" %% "zio-nio" % zionio
  , "dev.zio" %% "zio-akka-cluster" % zioakka
  )
).dependsOn(kvs_core)