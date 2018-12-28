package mws.rng
package store

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.{VectorClock}
import com.google.protobuf.ByteString
import leveldbjnr._
import mws.rng.data.{Data, SeqData, BucketInfo}
import mws.rng.msg.{StoreGet, StoreGetAck}
import mws.rng.msg_repl.{ReplGetBucketVc, ReplBucketVc, ReplGetBucketIfNew, ReplBucketUpToDate, ReplNewerBucketData, ReplBucketDataItem}
import mws.rng.msg_dump.{DumpGetBucketData, DumpBucketData, DumpBucketDataItem}
import mws.rng.msg_dump.{DumpGet, DumpEn}

object ReadonlyStore {
  def props(leveldb: LevelDB): Props = Props(new ReadonlyStore(leveldb))
}

class ReadonlyStore(leveldb: LevelDB) extends Actor with ActorLogging {
  val hashing = HashingExtension(context.system)
  val ro = new LevelDBReadOptions

  def get(k: Key): Option[Array[Byte]] = get(k.toByteArray)
  def get(k: Array[Byte]): Option[Array[Byte]] = Option(leveldb.get(k, ro))

  val `:key:` = stob(":key:")
  val `:keys` = stob(":keys")
  val `readonly_dummy` = stob("readonly_dummy")

  override def receive: Receive = {
    case StoreGet(key) =>
      val k = itob(hashing.findBucket(key)) ++ `:key:` ++ key
      val result: Seq[Data] = get(k).map(SeqData.parseFrom(_).data).getOrElse(Seq.empty[Data])
      sender ! StoreGetAck(result)

    case DumpGetBucketData(b) => 
      val k = itob(b) ++ `:keys`
      val b_info = get(k).map(BucketInfo.parseFrom(_))
      val keys: Seq[Key] = b_info.map(_.keys).getOrElse(Nil)
      val items: Seq[DumpBucketDataItem] = keys.map(key =>
        DumpBucketDataItem(
          key = key,
          data = get(itob(b)++`:key:`++key).map(SeqData.parseFrom(_).data).getOrElse(Vector.empty)
        )
      )
      sender ! DumpBucketData(b, items)
    case ReplGetBucketIfNew(b, vc) =>
      val vc_other: VectorClock = makevc(vc)
      val k = itob(b) ++ `:keys`
      val b_info = get(k).map(BucketInfo.parseFrom(_))
      b_info match {
        case Some(b_info) =>
          val vc_local: VectorClock = makevc(b_info.vc)
          vc_other == vc_local || vc_other > vc_local match {
            case true => sender ! ReplBucketUpToDate()
            case false =>
              val keys = b_info.keys
              val items: Seq[ReplBucketDataItem] = keys.map(key =>
                ReplBucketDataItem(
                  key = key,
                  data = get(itob(b)++`:key:`++key).map(SeqData.parseFrom(_).data).getOrElse(Vector.empty)
                )
              )
              sender ! ReplNewerBucketData(b_info.vc, items)
          }
        case None =>
          sender ! ReplBucketUpToDate()
      }
    case ReplGetBucketVc(b) =>
      val k = itob(b) ++ `:keys`
      val vc = get(k).map(BucketInfo.parseFrom(_).vc).getOrElse(Nil)
      sender ! ReplBucketVc(vc)

    case DumpGet(k) =>
      import java.io.{ByteArrayOutputStream, ObjectOutputStream, ObjectInputStream, ByteArrayInputStream}
      val key: Array[Byte] = {
        val bos = new ByteArrayOutputStream
        val out = new ObjectOutputStream(bos)
        out.writeObject(new String(k.toByteArray, "UTF-8"))
        out.close()
        bos.toByteArray
      }
      val data: Array[Byte] = leveldb.get(key, ro)
      val res: DumpEn = if (data == null) {
        DumpEn(k, `readonly_dummy`, ByteString.EMPTY)
      } else {
        val in = new ObjectInputStream(new ByteArrayInputStream(data))
        val obj = in.readObject
        in.close()
        val decoded = obj.asInstanceOf[(akka.util.ByteString, Option[String])] 
        DumpEn(k, atob(decoded._1.toArray), decoded._2.fold(ByteString.EMPTY)(stob))
      }
      sender ! res
    case _ =>    
  }

  override def postStop(): Unit = {
    ro.close()
    super.postStop()
  }
}
