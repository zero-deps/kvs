package mws.rng
package store

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.{VectorClock}
import leveldbjnr._
import mws.rng.data.{Data, BucketInfo}
import mws.rng.data.codec._
import mws.rng.model.{StoreGet, StoreGetAck}
import mws.rng.model.{DumpGet, DumpEn}
import mws.rng.model.{DumpGetBucketData, DumpBucketData}
import mws.rng.model.{ReplGetBucketsVc, ReplBucketsVc, ReplGetBucketIfNew, ReplBucketUpToDate, ReplNewerBucketData}
import scala.collection.{breakOut}
import zd.proto.api.decode

object ReadonlyStore {
  def props(leveldb: LevelDB): Props = Props(new ReadonlyStore(leveldb))
}

class ReadonlyStore(leveldb: LevelDB) extends Actor with ActorLogging {
  val hashing = HashingExtension(context.system)
  val ro = new LevelDBReadOptions

  def get(k: Key): Option[Array[Byte]] = Option(leveldb.get(k, ro))

  val `:key:` = stob(":key:")
  val `:keys` = stob(":keys")
  val `readonly_dummy` = stob("readonly_dummy")

  override def receive: Receive = {
    case StoreGet(key) =>
      val k = itob(hashing.findBucket(key)) ++ `:key:` ++ key
      val result: Option[Data] = get(k).map(decode[Data](_))
      sender ! StoreGetAck(result)

    case DumpGetBucketData(b) => 
      val k = itob(b) ++ `:keys`
      val b_info = get(k).map(decode[BucketInfo](_))
      val keys: Vector[Key] = b_info.map(_.keys.toVector).getOrElse(Vector.empty)
      val items: Vector[Data] = keys.flatMap(key =>
        get(itob(b)++`:key:`++key).map(decode[Data](_))
      )
      sender ! DumpBucketData(b, items)
    case ReplGetBucketIfNew(b, vc) =>
      val vc_other: VectorClock = vc
      val k = itob(b) ++ `:keys`
      val b_info = get(k).map(decode[BucketInfo](_))
      b_info match {
        case Some(b_info) =>
          val vc_local: VectorClock = b_info.vc
          vc_other == vc_local || vc_other > vc_local match {
            case true => sender ! ReplBucketUpToDate
            case false =>
              val keys = b_info.keys
              val items: Vector[Data] = keys.flatMap(key =>
                get(itob(b)++`:key:`++key).map(decode[Data](_))
              )(breakOut)
              sender ! ReplNewerBucketData(b_info.vc, items)
          }
        case None =>
          sender ! ReplBucketUpToDate
      }
    case ReplGetBucketsVc(bs) =>
      val bvcs: Bucket Map VectorClock = bs.flatMap{ b =>
        val k = itob(b) ++ `:keys`
        get(k).map(x => b -> decode[BucketInfo](x).vc)
      }(breakOut)
      sender ! ReplBucketsVc(bvcs)

    case DumpGet(k) =>
      import java.io.{ByteArrayOutputStream, ObjectOutputStream, ObjectInputStream, ByteArrayInputStream}
      val key: Array[Byte] = {
        val bos = new ByteArrayOutputStream
        val out = new ObjectOutputStream(bos)
        out.writeObject(new String(k, "UTF-8"))
        out.close()
        bos.toByteArray
      }
      val data: Array[Byte] = leveldb.get(key, ro)
      val res: DumpEn = if (data == null) {
        DumpEn(k, `readonly_dummy`, Array.empty[Byte])
      } else {
        val in = new ObjectInputStream(new ByteArrayInputStream(data))
        val obj = in.readObject
        in.close()
        val decoded = obj.asInstanceOf[(akka.util.ByteString, Option[String])] 
        DumpEn(k, decoded._1.toArray, decoded._2.fold(Array.empty[Byte])(stob))
      }
      sender ! res
    case _ =>    
  }

  override def postStop(): Unit = {
    ro.close()
    super.postStop()
  }
}
