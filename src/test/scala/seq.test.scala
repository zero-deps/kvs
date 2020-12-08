package kvs.seq

import zio._
import zd.proto._, api._, macrosapi._

import kvs.store.Rng.{Conf=>RngConf}

case class Data(@N(1) i: Int)
object Data {
  implicit val dataCodec = caseCodecAuto[Data]
}

package object test {
  val v1 = Data(1)
  val v2 = Data(2)
  val v3 = Data(3)
  val v4 = Data(4)

  def kvsService(port: Int, dir: String) = {
    val testConf = """
      akka.loglevel=off
      akka.cluster.jmx.multi-mbeans-in-same-jvm = on
    """
    val akkaConf    = ActorSystem.staticConf("KvsActorSystem", "127.0.0.1", port, testConf)
    val actorSystem = akkaConf >>> ActorSystem.live.orDie
    val dbaConf     = Dba.rngConf(RngConf(dir=s"data/test-$dir"))
    val dba         = actorSystem ++ dbaConf >>> Dba.live.orDie
    val kvs         = actorSystem ++ dba     >>> Kvs.live.orDie
    kvs
  }
}
