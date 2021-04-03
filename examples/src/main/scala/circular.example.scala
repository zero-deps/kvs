package example
package circular

import kvs.seq._, kvs.store.Rng.{Conf=>RngConf}
import proto._, macrosapi._
import zio._, zio.console._
import zio.blocking.Blocking
import zio.clock.Clock

object App {
  def main(args: Array[String]): Unit = {
    val runtime = Runtime.default

    val akkaConf    = ActorSystem.staticConf("KvsActorSystem", "127.0.0.1", 4343, "akka.loglevel=off")
    val actorSystem = akkaConf >>> ActorSystem.live.orDie
    val dbaConf     = Dba.rngConf(RngConf(dir="../data/example-circular"))
    val dba         = actorSystem ++ dbaConf ++ ZLayer.requires[Clock] >>> Dba.live
    val kvs         = actorSystem ++ dba ++ ZLayer.requires[Clock with Blocking] >+> Kvs.live

    val app =
      for {
        _    <- Kvs.circular.add(Fid, Data())
        _    <- putStrLn("all done.")
      } yield ()

    runtime.unsafeRun(app.provideCustomLayer(kvs))
  }
}

case class Data()
object Data {
  implicit val dataCodec: MessageCodec[Data] = caseCodecAuto[Data]
}

case object Fid {
  type Fid = Fid.type
  implicit val fid2Codec = caseCodecAuto[Fid]
  implicit val fid2KvsFeed: KvsCircular.Buffer[Fid, Data] = KvsCircular.buffer(Bytes.unsafeWrap(Array[Byte](3)), 10)
}
