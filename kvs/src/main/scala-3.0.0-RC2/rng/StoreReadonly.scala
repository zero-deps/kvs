package zd.rng
package store

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.{VectorClock}
import leveldbjnr.*
import zd.rng.data.{Data, BucketInfo}
import zd.rng.data.codec.*
import zd.rng.model.{StoreGet, StoreGetAck}
import zd.rng.model.{DumpGet, DumpEn}
import zd.rng.model.{DumpGetBucketData, DumpBucketData}
import zd.rng.model.{ReplGetBucketsVc, ReplBucketsVc, ReplGetBucketIfNew, ReplBucketUpToDate, ReplNewerBucketData}
import proto.*

object ReadonlyStore {
  def props(leveldb: LevelDb): Props = Props(new ReadonlyStore(leveldb))
}

class ReadonlyStore(leveldb: LevelDb) extends Actor with ActorLogging {
  val hashing = HashingExtension(context.system)
  val ro = ReadOpts()

  def get(k: Key): Option[Array[Byte]] = leveldb.get(k, ro).fold(l => throw l, r => r)

  val `:key:` = stob(":key:")
  val `:keys` = stob(":keys")
  val `readonly_dummy` = stob("readonly_dummy")

  override def receive: Receive = {
    case x: StoreGet =>
      val k = itob(hashing.findBucket(x.key)) ++ `:key:` ++ x.key
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
              val items: Vector[Data] = keys.view.flatMap(key =>
                get(itob(b)++`:key:`++key).map(decode[Data](_))
              ).to(Vector)
              sender ! ReplNewerBucketData(b_info.vc, items)
          }
        case None =>
          sender ! ReplBucketUpToDate
      }
    case ReplGetBucketsVc(bs) =>
      val bvcs: Bucket Map VectorClock = bs.view.flatMap{ b =>
        val k = itob(b) ++ `:keys`
        get(k).map(x => b -> decode[BucketInfo](x).vc)
      }.to(Map)
      sender ! ReplBucketsVc(bvcs)

    case x: DumpGet =>
      import java.io.{ByteArrayOutputStream, ObjectOutputStream, ObjectInputStream, ByteArrayInputStream}
      val key: Array[Byte] = {
        val bos = new ByteArrayOutputStream
        val out = new ObjectOutputStream(bos)
        out.writeObject(new String(x.k, "UTF-8"))
        out.close()
        bos.toByteArray.nn
      }
      val data: Option[Array[Byte]] = leveldb.get(key, ro).fold(l => throw l, r => r)
      val res: DumpEn = data match {
        case None =>
          DumpEn(x.k, `readonly_dummy`, Array.empty[Byte])
        case Some(data) =>
          val in = new ObjectInputStream(new ByteArrayInputStream(data))
          val obj = in.readObject
          in.close()
          val decoded = obj.asInstanceOf[(akka.util.ByteString, Option[String])] 
          DumpEn(x.k, decoded._1.toArray, decoded._2.fold(Array.empty[Byte])(stob))
      }
      sender ! res
    case _ =>    
  }

  override def postStop(): Unit = {
    ro.close()
    super.postStop()
  }
}
