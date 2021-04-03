package example
package ring

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
    val dbaConf     = Dba.rngConf(RngConf(dir="../data/example-feed"))
    val dba         = actorSystem ++ dbaConf ++ ZLayer.requires[Clock] >>> Dba.live
    val kvs         = actorSystem ++ dba ++ ZLayer.requires[Clock with Blocking] >+> Kvs.live

    val app =
      for {
        _    <- Kvs.feed.put(Feed1, Key1(1), Data())
//      _    <- Kvs.feed.add(Feed1, Data()) // illegal api usage, compile err
        key2 <- Kvs.feed.add(Feed2, Data())
        _     = key2: Key2
//      _    <- Kvs.feed.put(Feed2, key2, Data()) // illegal api usage, compile err
        _    <- putStrLn("all done.")
      } yield ()

    runtime.unsafeRun(app.provideCustomLayer(kvs))
  }
}

case class Data()
object Data {
  implicit val dataCodec: MessageCodec[Data] = caseCodecAuto[Data]
}

case class Key1(@N(1) x: Int)
object Key1 {
  implicit val key1Codec = caseCodecAuto[Key1]
}
case object Feed1 {
  type Feed1 = Feed1.type
  implicit val fid1Codec = caseCodecAuto[Feed1]
  import Fid._
  implicit val fid1KvsFeed: KvsFeed.Manual[Feed1, Key1, Data] = KvsFeed.manualFeed(encodeToBytes[Fid](Fid1()))
}

case class Key2 private (bytes: Bytes)
case object Feed2 {
  type Feed2 = Feed2.type
  implicit val fid2Codec = caseCodecAuto[Feed2]
  import Fid._
  implicit val fid2KvsFeed: KvsFeed.Increment[Feed2, Key2, Data] = KvsFeed.incrementFeed(encodeToBytes[Fid](Fid2()), _.bytes, Key2.apply)
}

sealed trait Fid
@N(1) case class Fid1() extends Fid
@N(2) case class Fid2() extends Fid
object Fid {
  implicit val fid1c = caseCodecAuto[Fid1]
  implicit val fid2c = caseCodecAuto[Fid2]
  implicit val fidc: MessageCodec[Fid] = sealedTraitCodecAuto[Fid]
}
