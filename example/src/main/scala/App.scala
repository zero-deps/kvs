package example

import zd.proto.api.{N, encode, decode, encodeToBytes, MessageCodec}
import zd.proto.Bytes
import zd.proto.macrosapi.{caseCodecAuto, sealedTraitCodecAuto}
import zio.Runtime

import kvs.seq._

object App {
  def main(args: Array[String]): Unit = {
    val runtime = Runtime.default

    val actorSystem = ActorSystem.live("KvsActorSystem", "127.0.0.1", 4343).orDie
    val dbaConf     = Dba.ringConf()
    val dba         = actorSystem ++ dbaConf >>> Dba.live.orDie
    val kvs         = actorSystem ++ dba >>> Kvs.live.orDie

    val app =
      for {
        _ <- Kvs.list.put(Fid1(), Key1(), Item1()).orDie
        _ <- KvsList.put(Fid1(), Key1(), Item1()).orDie
        _ <- Kvs.array.put(Fid2(), 1, Item1()).orDie
        _ <- KvsArray.put(Fid2(), 1, Item1()).orDie
      } yield ()
    runtime.unsafeRun(app.provideLayer(kvs))
  }
}

case class Key1()
object Key1 {
  implicit val key1C: MessageCodec[Key1] = caseCodecAuto[Key1]
}

case class Item1()
object Item1 {
  implicit val item1C: MessageCodec[Item1] = caseCodecAuto[Item1]
}

case class Item2()
object Item2 {
  implicit val item2C: MessageCodec[Item2] = caseCodecAuto[Item2]
}

case class Fid1()
object Fid1 {
  implicit val fid1C: MessageCodec[Fid1] = caseCodecAuto[Fid1]
  implicit val fid1Info: ListFid[Fid1, Key1, Item1] = ListFid()
}
case class Fid2()
object Fid2 {
  implicit val fid2C: MessageCodec[Fid2] = caseCodecAuto[Fid2]
  implicit val fid2Info: ArrayFid[Fid2, Item1] = ArrayFid(size = 10)
}
