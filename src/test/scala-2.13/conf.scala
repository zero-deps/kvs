package zd.kvs

object conf {
  def tmpl(port: Int) = s"""
    |akka {
    |  loglevel = off
    |
    |  actor.provider = cluster
    |  
    |  remote.artery.canonical {
    |    hostname = 127.0.0.1
    |    port = ${port}
    |  }
    |
    |  cluster {  
    |    seed-nodes = [
    |      "akka.tcp://Test@127.0.0.1:${port}",
    |    ]
    |  }
    |}
    |
    |ring.leveldb.dir = "rng_data_test_${port}"
  """.stripMargin
}
