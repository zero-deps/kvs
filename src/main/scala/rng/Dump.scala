package mws.rng

import akka.actor.{Actor, ActorLogging, ActorRef, Props, PoisonPill}
import akka.pattern.ask
import akka.util.Timeout
import java.text.SimpleDateFormat
import java.util.Calendar
import mws.rng.data.{Data}
import mws.rng.msg_dump.{DumpBucketData, DumpGetBucketData}
import scala.collection.immutable.{SortedMap}
import scala.concurrent.duration._
import scala.concurrent.{Await}
import scalaz.Scalaz._

object DumpProcessor {
  def props(): Props = Props(new DumpProcessor)

  final case class Load(path: String)
  final case class Save(buckets: SortedMap[Bucket, PreferenceList], local: Node, path: String)
}

class DumpProcessor extends Actor with ActorLogging {
  implicit val timeout = Timeout(120 seconds)
  val maxBucket: Bucket = context.system.settings.config.getInt("ring.buckets") - 1
  val stores = SelectionMemorize(context.system)
  def receive = waitForStart

  def waitForStart: Receive = {
    case DumpProcessor.Load(path) =>
      val dumpIO = context.actorOf(DumpIO.props(path))
      dumpIO ! DumpIO.ReadNext
      context.become(loadDump(dumpIO, sender)())

    case DumpProcessor.Save(buckets, local, path) =>
      val timestamp = new SimpleDateFormat("yyyy.MM.dd-HH.mm.ss").format(Calendar.getInstance().getTime)
      val dumpPath = s"${path}/rng_dump_${timestamp}"
      val dumpIO = context.actorOf(DumpIO.props(dumpPath))
      buckets(0).foreach(n => stores.get(n, "ring_readonly_store").fold(
        _ ! DumpGetBucketData(0),
        _ ! DumpGetBucketData(0),
      ))
      context.become(saveDump(buckets, local, dumpIO, sender)())
  }

  def loadDump(dumpIO: ActorRef, client: ActorRef): () => Receive = {
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
        if (res.last) {
          log.info(s"load info: load is completed, total keys=${keysNumber}, size=${size}, ksize=${ksize}".green)
        } else if (keysNumber % 1000 == 0) {
          log.info(s"load info: write done, total keys=${keysNumber}, size=${size}, ksize=${ksize}".green)
        }
        if (res.last) {
          client ! "done"
          stores.get(self.path.address, "ring_hash").fold(_ ! RestoreState, _ ! RestoreState)
          dumpIO ! PoisonPill
          context.stop(self)
        }
    }
  }

  def saveDump(buckets: SortedMap[Bucket, PreferenceList], local: Node, dumpIO: ActorRef, client: ActorRef): () => Receive = {
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
        client ! "failed"
        context.stop(dumpIO)
        context.stop(self)
      case DumpIO.PutDone(path) =>
        if (putQueue.isEmpty) {
          if (processBucket == maxBucket) {
            log.info("dump write done".green)
            stores.get(self.path.address, "ring_hash").fold(
              _ ! RestoreState,
              _ ! RestoreState
            )
            client ! path
            dumpIO ! PoisonPill
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
