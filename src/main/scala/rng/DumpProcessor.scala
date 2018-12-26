package mws.rng

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.google.protobuf.{ByteString}
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit
import leveldbjnr._
import mws.kvs.LeveldbOps
import mws.rng.data.{Data}
import mws.rng.data_dump.{DumpKV, KV}
import mws.rng.msg_dump.{DumpPut, DumpGet, DumpEn, DumpGetBucketData, DumpBucketData, DumpBucketDataItem}
import mws.rng.store._
import scala.annotation.tailrec
import scala.collection.immutable.{SortedMap, SortedSet}
import scala.concurrent.{Await}
import scala.util.Try
import scalaz.Scalaz._

object DumpProcessor {
  def props: Props = Props(new DumpProcessor)

  final case class LoadDump(path: String)
  final case class SaveDump(buckets: SortedMap[Bucket, PreferenceList], local: Node, path: String)
}

class DumpProcessor extends Actor with ActorLogging {
  implicit val timeout = Timeout(120, TimeUnit.SECONDS)
  val maxBucket: Bucket = context.system.settings.config.getInt("ring.buckets") - 1
  val stores = SelectionMemorize(context.system)
  def receive = waitForStart

  def waitForStart: Receive = {
    case ld: DumpProcessor.LoadDump =>
      val dumpIO = context.actorOf(DumpIO.props(ld.path))
      dumpIO ! DumpIO.ReadNext
      context.become(loadDump(dumpIO, sender)())

    case sd: DumpProcessor.SaveDump =>
      val timestamp = new SimpleDateFormat("yyyy.MM.dd-HH.mm.ss").format(Calendar.getInstance().getTime)
      val dumpPath = s"${sd.path}/rng_dump_${timestamp}"
      val dumpIO = context.actorOf(DumpIO.props(dumpPath))
      sd.buckets(0).foreach(n => stores.get(n, "ring_readonly_store").fold(
        _ ! DumpGetBucketData(0),
        _ ! DumpGetBucketData(0),
      ))
      context.become(saveDump(sd.buckets, sd.local, dumpIO, sender)())
  }

  def loadDump(dumpIO: ActorRef, dumpInitiator: ActorRef): () => Receive = {
    var keysNumber: Long = 0L
    var size: Long = 0L
    var ksize: Long = 0L
    () => {
      case res: DumpIO.ReadNextRes =>
        if (!res.last) dumpIO ! DumpIO.ReadNext
        keysNumber = keysNumber + res.kv.size
        res.kv.foreach { d =>
          ksize = ksize + d._1.size
          size = size + d._2.size
          val putF = stores.get(self.path.address, "ring_hash").fold(_.ask(InternalPut(d._1, d._2)), _.ask(InternalPut(d._1, d._2)))
          Await.ready(putF, timeout.duration)
        }
        if (keysNumber % 1000 == 0) {
          log.info(s"load info: write done, total keys=${keysNumber}, size=${size}, ksize=${ksize}")
        }
        if (res.last) {
          log.info(s"load info: load is completed, total keys=${keysNumber}, size=${size}, ksize=${ksize}")
          dumpInitiator ! "done"
          stores.get(self.path.address, "ring_hash").fold(_ ! RestoreState, _ ! RestoreState)
          context.stop(self)
        }
    }
  }

  def saveDump(buckets: SortedMap[Bucket, PreferenceList], local: Node, dumpIO: ActorRef, dumpInitiator: ActorRef): () => Receive = {
    var processBucket: Int = 0
    var keysNumber: Long = 0
    var collected: Seq[Seq[Data]] = Seq.empty
    
    var putQueue: Seq[DumpIO.Put] = Seq.empty
    var readyToPut: Boolean = true
    var pullWorking: Boolean = false

    def pull: Unit = {
      if (!pullWorking && putQueue.size < 50 && processBucket < maxBucket) {
        processBucket = processBucket + 1
        pullWorking = true
        buckets(processBucket).foreach(n => stores.get(n, "ring_readonly_store").fold(
          _ ! DumpGetBucketData(processBucket),
          _ ! DumpGetBucketData(processBucket),
        ))  
      }
    }

    def showInfo(msg: String): Unit = {
      if (processBucket === maxBucket && putQueue.isEmpty) {
        log.info(s"dump done: msg=${msg}, bucket=${processBucket}/${maxBucket}, total=${keysNumber}, putQueue=${putQueue.size}")
      } else if (keysNumber % 10000 == 0) {
        log.info(s"dump info: msg=${msg}, bucket=${processBucket}/${maxBucket}, total=${keysNumber}, putQueue=${putQueue.size}")
      }
    }

    () => {
      case res: (DumpBucketData) if processBucket === res.b =>
        val res_l: Seq[Data] = res.items.flatMap(_.data) //todo: replace `res_l` with `items`
        collected = res_l +: collected
        if (collected.size === buckets(processBucket).size) {
          pullWorking = false
          pull

          val merged: Seq[Data] = mergeBucketData(collected.flatten)
          collected = Seq.empty
          keysNumber = keysNumber + merged.size
          if (readyToPut) {
            readyToPut = false
            dumpIO ! DumpIO.Put(merged)
          } else {
            putQueue = DumpIO.Put(merged) +: putQueue
          }
          showInfo("main")
        }
      case res: DumpBucketData =>
        log.error(s"wrong bucket response, expected=${processBucket}, actual=${res.b}")
        stores.get(self.path.address, "ring_hash").fold(
          _ ! RestoreState,
          _ ! RestoreState,
        )
        context.stop(self)
      case DumpIO.PutDone =>
        if (putQueue.isEmpty) {
          if (processBucket == maxBucket) {
            log.info("dump write done")
            stores.get(self.path.address, "ring_hash").fold(
              _ ! RestoreState,
              _ ! RestoreState
            )
            context.stop(self)
          }
          readyToPut = true
        } else {
          putQueue.headOption.map(dumpIO ! _)
          putQueue = putQueue.tail
        }
        pull
        showInfo("io")
    }
  }
}
