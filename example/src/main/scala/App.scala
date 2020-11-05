package example

import kvs.seq.{Kvs, KvsList, KvsArray, Dba, ActorSystem}
import zd.proto.Bytes
import zd.proto.api.{N, encode, decode, encodeToBytes, MessageCodec}
import zd.proto.macrosapi.caseCodecAuto
import zio.Runtime

object App {
  def main(args: Array[String]): Unit = {
    val runtime = Runtime.default

    val akkaConf    = ActorSystem.staticConf("KvsActorSystem", "127.0.0.1", 4343)
    val actorSystem = akkaConf >>> ActorSystem.live.orDie
    val dbaConf     = Dba.ringConf()
    val dba         = actorSystem ++ dbaConf >>> Dba.live.orDie
    val kvs         = actorSystem ++ dba >>> Kvs.live.orDie

    val app =
      for {
        _    <- Kvs.list.prepend(Fid1(), Key1("el1"), Data())
        // _    <- Kvs.list.prepend(Fid1(), Data()) // illegal api usege, compile err
        key2 <- Kvs.list.prepend(Fid2, Data())
        _    = key2: Key2
        // _    <- Kvs.list.prepend(Fid2, key2, Data()) // illegal api usege, compile err
        _    <- Kvs.array.add(Fid3, Data())
      } yield ()

    runtime.unsafeRun(app.provideLayer(kvs))
  }
}

case class Data()
object Data {
  implicit val dataCodec: MessageCodec[Data] = caseCodecAuto[Data]
}

case class Key1(@N(1) a: String)
object Key1 {
  implicit val key1C: MessageCodec[Key1] = caseCodecAuto[Key1]
}
case class Fid1()
object Fid1 {
  implicit val fid1Codec: MessageCodec[Fid1] = caseCodecAuto[Fid1]
  implicit val fid1KvsList: KvsList.Manual[Fid1, Key1, Data] = KvsList.manualFeed("Fid1")
}

case class Key2 private(bytes: Bytes) extends AnyVal
case object Fid2 {
  implicit val fid2Codec: MessageCodec[Fid2.type] = caseCodecAuto[Fid2.type]
  implicit val fid2KvsList: KvsList.Increment[Fid2.type, Key2, Data] = KvsList.incrementFeed("Fid2", _.bytes, Key2.apply)
}

case object Fid3 {
  implicit val fid2Codec: MessageCodec[Fid3.type] = caseCodecAuto[Fid3.type]
  implicit val fid2KvsList: KvsArray.Feed[Fid3.type, Data] = KvsArray.feed("Fid3", 10)
}
