package mws.rng
package store

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.{Cluster, VectorClock}
import leveldbjnr._
import mws.rng.data.{Data, BucketInfo}
import mws.rng.data_dump.{ValueKey}
import mws.rng.msg.{StorePut, StoreDelete}
import mws.rng.msg_repl.{ReplBucketPut}
import mws.rng.msg_dump.{DumpPut}
import scalaz.Scalaz._

class WriteStore(leveldb: LevelDB) extends Actor with ActorLogging {
  import context.system
  val config = system.settings.config.getConfig("ring.leveldb")

  val ro = new LevelDBReadOptions
  val wo = new LevelDBWriteOptions
  wo.setSync(config.getBoolean("fsync"))
  val hashing = HashingExtension(system)

  val local: Node = Cluster(system).selfAddress

  def get(k: Key): Option[Array[Byte]] = get(k.toByteArray)
  def get(k: Array[Byte]): Option[Array[Byte]] = Option(leveldb.get(k, ro))

  val `:key:` = stob(":key:")
  val `:keys` = stob(":keys")

  override def postStop(): Unit = {
    leveldb.close()
    ro.close()
    wo.close()
    super.postStop()
  }

  def receive: Receive = {
    case StorePut(data) => 
      doPut(data)
      sender ! "ok"
    case DumpPut(k: Key, v: Value, nextKey: Key) =>
      withBatch(_.put(k.toByteArray, ValueKey(v=v, nextKey=nextKey).toByteArray))
      sender ! "done"
    case StoreDelete(data) => sender ! doDelete(data)
    case ReplBucketPut(b, bucketVc, items) => replBucketPut(b, bucketVc, items.toVector)
    case unhandled => log.warning(s"unhandled message: ${unhandled}")
  }

  def replBucketPut(b: Bucket, bucketVc: VectorClockList, items: Vector[Data]): Unit = {
    withBatch{ batch =>
      { // updating bucket info
        val bucketId: Key = itob(b) ++ `:keys`
        val bucketInfo = get(bucketId).map(BucketInfo.parseFrom(_))
        val newKeys = items.map(_.key)
        val v = bucketInfo match {
          case Some(x) =>
            BucketInfo(vc=bucketVc, keys=(newKeys++x.keys).distinct)
          case None =>
            BucketInfo(vc=bucketVc, keys=newKeys.distinct)
        }
        batch.put(bucketId.toByteArray, v.toByteArray)
      }
      // saving keys data
      items.foreach{ data =>
        val keyPath: Key = itob(b) ++ `:key:` ++ data.key
        val keyData: Option[Data] = get(keyPath).map(Data.parseFrom(_))
        val v: Option[Data] = MergeOps.forPut(stored=keyData, received=data)
        v.map(v => batch.put(keyPath.toByteArray, v.toByteArray))
      }
    }
  }

  def doPut(data: Data): Unit = {
    withBatch{ batch =>
      { // updating bucket info
        val bucketId: Key = itob(data.bucket) ++ `:keys`
        val bucketInfo = get(bucketId).map(BucketInfo.parseFrom(_))
        val v = bucketInfo match {
          case Some(x) if x.keys contains data.key =>
            val vc = fromvc(makevc(x.vc) :+ local.toString)
            x.copy(vc=vc)
          case Some(x) =>
            val vc = fromvc(makevc(x.vc) :+ local.toString)
            x.copy(vc=vc, keys=(data.key +: x.keys))
          case None =>
            val vc = fromvc(new VectorClock() :+ local.toString)
            BucketInfo(vc=vc, keys=Vector(data.key))
        }
        batch.put(bucketId.toByteArray, v.toByteArray)
      }
      // saving key data
      val keyPath: Key = itob(data.bucket) ++ `:key:` ++ data.key
      val keyData: Option[Data] = get(keyPath).map(Data.parseFrom(_))
      val v: Option[Data] = MergeOps.forPut(stored=keyData, received=data)
      v.map(v => batch.put(keyPath.toByteArray, v.toByteArray))
    }
  }

  def doDelete(key: Key): String = {
    val b = hashing.findBucket(key)
    val b_info = get(itob(b) ++ `:keys`).map(BucketInfo.parseFrom(_))
    b_info match {
      case Some(b_info) =>
        val vc = fromvc(makevc(b_info.vc) :+ local.toString)
        val keys = b_info.keys.filterNot(_ === key)
        withBatch(batch => {
          batch.delete((itob(b) ++ `:key:` ++ key).toByteArray)
          batch.put((itob(b) ++ `:keys`).toByteArray, BucketInfo(vc, keys).toByteArray)
        })
        "ok"
      case None => 
        "ok"
    }
  }

  def withBatch[R](body: LevelDBWriteBatch ⇒ R): R = {
    val batch = new LevelDBWriteBatch
    try {
      val r = body(batch)
      leveldb.write(batch,wo)
      r
    } finally {
      batch.close()
    }
  }
}

object WriteStore {
  def props(leveldb: LevelDB): Props = Props(new WriteStore(leveldb))
}

