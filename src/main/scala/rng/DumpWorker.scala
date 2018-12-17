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
import mws.rng.data.{Data, DumpKV, KV}
import mws.rng.msg.{BucketGet, GetBucketResp, PutSavingEntity, GetSavingEntity, SavingEntity}
import mws.rng.store._
import scala.annotation.tailrec
import scala.collection.immutable.{SortedMap, SortedSet}
import scala.concurrent.{Await}
import scala.util.Try
import scalaz.Scalaz._

final case class DumpData(current: Bucket, prefList: PreferenceList, collected: Seq[Seq[Data]], lastKey: Option[Key], client: Option[ActorRef])

object DumpWorker {
  def props(buckets: SortedMap[Bucket, PreferenceList], local: Node, path: String): Props = Props(new DumpWorker(buckets, local, path))
}

class DumpWorker(buckets: SortedMap[Bucket, PreferenceList], local: Node, path: String) extends FSM[FsmState, DumpData] with ActorLogging {
    implicit val ord = Ordering.by[Node, String](_.hostPort)

    val timestamp = new SimpleDateFormat("yyyy.MM.dd-HH.mm.ss").format(Calendar.getInstance().getTime)
    val filePath = s"${path}/rng_dump_${timestamp}"
    var db: LevelDB = _
    var dumpStore: ActorRef = _
    val stores = SelectionMemorize(context.system)
    val maxBucket: Bucket = context.system.settings.config.getInt("ring.buckets")

    var keysInDump: Int = 0
    implicit val timeout = Timeout(120, TimeUnit.SECONDS)

    override def preRestart (reason: Throwable, message: Option[Any]): Unit = {
      stores.get(self.path.address, "ring_hash").fold(_ ! RestoreState, _ ! RestoreState)
      log.info(s"Dump creating is failed. Reason : ${reason.getMessage}")
      super.preRestart(reason, message)
    }

    startWith(ReadyCollect, DumpData(0, SortedSet.empty[Node], Seq.empty, None, None))

    when(ReadyCollect){
      case Event(Dump(_), state) =>
        db = LeveldbOps.open(context.system, filePath)
        dumpStore = context.actorOf(Props(classOf[WriteStore], db))
        buckets(state.current).foreach(n => stores.get(n, "ring_readonly_store").fold(
          _ ! BucketGet(state.current),
          _ ! BucketGet(state.current),
        ))
        goto(Collecting) using DumpData(state.current, buckets(state.current), Seq.empty, None, Some(sender))
    }

    when(Collecting){
      case Event(GetBucketResp(b,data), state) => // TODO add timeout if any node is not responding.
        val pList = state.prefList - (if(sender().path.address.hasLocalScope) local else sender().path.address)
        if (pList.isEmpty) {
          val merged: Seq[Data] = mergeBucketData((data +: state.collected).foldLeft(Seq.empty[Data])((acc, l) => l ++ acc), Seq.empty)
          val lastKey: Option[Key] = if (merged.size == 0) {
            state.lastKey
          } else {
            linkKeysInDb(merged, state.lastKey)
          }
          b+1 match {
            case `maxBucket` =>
              stores.get(self.path.address, "ring_hash").fold(_ ! RestoreState, _ ! RestoreState)
              log.info(s"Dump complete, sending path to hash, lastKey = $lastKey, keysInDump=$keysInDump")
              dumpStore ! PutSavingEntity(stob("head_of_keys"), stob("dummy"), lastKey.getOrElse(ByteString.EMPTY))
              dumpStore ! PoisonPill //TODO stop after processing last msg
              Thread.sleep(5000)
              log.info(s"dump ok=$filePath")
              state.client.map(_ ! s"$filePath")
              Try(db.close()).recover{ case err => log.info(s"Error closing db ${err}")}
              stop()
            case nextBucket =>
              buckets(nextBucket).foreach(n => stores.get(n, "ring_readonly_store").fold(
                _ ! BucketGet(nextBucket),
                _ ! BucketGet(nextBucket),
              ))
              stay() using DumpData(nextBucket, buckets(nextBucket), Nil, lastKey, state.client)
          }
        } else {
          stay() using DumpData(state.current, pList, data +: state.collected, state.lastKey, state.client)
        }
    }

    @tailrec
    final def linkKeysInDb(ldata: Seq[Data], prevKey: Option[Key]): Option[Key] = {
      ldata.headOption match {
        case None => prevKey
        case Some(h) =>
          log.debug("dump key={}", h.key)
          Await.ready(dumpStore ? PutSavingEntity(h.key, h.value, prevKey.getOrElse(ByteString.EMPTY)), timeout.duration)
          keysInDump = keysInDump + 1
          linkKeysInDb(ldata.tail, Some(h.key))
      }
    }

    initialize()
}

object LoadDumpWorkerJava {
  def props(path: String): Props = Props(new LoadDumpWorkerJava(path))
}

class LoadDumpWorkerJava(path: String) extends FSM[FsmState, Option[ActorRef]] with ActorLogging {
  implicit val timeout = Timeout(120, TimeUnit.SECONDS)
  var keysNumber = 0
  var size: Long = 0L
  var ksize: Long = 0L

  var dumpDb: LevelDB = _
  var store: ActorRef = _
  val stores = SelectionMemorize(context.system)
  startWith(ReadyCollect, None)

  when(ReadyCollect){
    case Event(LoadDump(_, _),_) =>
      dumpDb = LeveldbOps.open(context.system, path)
      store = context.actorOf(Props(classOf[ReadonlyStore], dumpDb))
      store ! GetSavingEntity(stob("head_of_keys"))
      goto(Collecting) using Some(sender)
  }

  when(Collecting){
    case Event(SavingEntity(k,v,nextKey),state) =>
      log.debug("saving state {} -> {}, nextKey = {}", k, v, nextKey)
      if (!nextKey.isEmpty) {
        store ! GetSavingEntity(nextKey)
      }
      size = size + v.size
      ksize = ksize + k.size
      val putF = stores.get(self.path.address, "ring_hash").fold(
        _.ask(InternalPut(k,v)),
        _.ask(InternalPut(k,v)),
      )
      Await.ready(putF, timeout.duration)
      if (nextKey.isEmpty) {
        stores.get(self.path.address, "ring_hash").fold(_ ! RestoreState, _ ! RestoreState)
        Try(dumpDb.close()).recover{ case err => log.info(s"Error closing db $err")}
        log.info("load is completed, keys={}", keysNumber)
        state.map(_ ! "done")
        stop()
      } else {
        keysNumber = keysNumber + 1
        if (keysNumber % 10000 == 0) log.info(s"load info: write keys=${keysNumber}, size=${size}, ksize=${ksize}, nextKey=${nextKey}")
        stay() using state
      }
  }
}

object IterateDumpWorker {
  def props(path: String, foreach: (ByteString,ByteString) => Unit): Props = Props(new IterateDumpWorker(path, foreach))
}
class IterateDumpWorker(path: String, foreach: (ByteString,ByteString) => Unit) extends FSM[FsmState, Option[ActorRef]] with ActorLogging {
  val extraxtedDir = path

  var dumpDb: LevelDB = _
  var store: ActorRef = _
  val stores = SelectionMemorize(context.system)
  startWith(ReadyCollect, None)

  when(ReadyCollect){
    case Event(IterateDump(_,_),_) =>
      dumpDb = LeveldbOps.open(context.system, extraxtedDir)
      store = context.actorOf(Props(classOf[ReadonlyStore], dumpDb))
      store ! GetSavingEntity(stob("head_of_keys"))
      goto(Collecting) using Some(sender)
  }

  when(Collecting){
    case Event(SavingEntity(k,v,nextKey),state) =>
      log.debug(s"iterate key=$k")
      foreach(k,v)
      if (nextKey.isEmpty) {
        Try(dumpDb.close()).recover{ case err => log.info(s"Error closing db $err")}
        state.map(_ ! "done")
        log.debug("iteration ended")
        stop()
      } else {
        store ! GetSavingEntity(nextKey)
        stay() using state
      }
  }
}

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
      sd.buckets(0).foreach(n => stores.get(n, "ring_readonly_store").fold(_ ! BucketGet(0), _ ! BucketGet(0)))
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
        buckets(processBucket).foreach(n => stores.get(n, "ring_readonly_store").fold(_ ! BucketGet(processBucket), _ ! BucketGet(processBucket)))  
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
      case res: (GetBucketResp) if processBucket == res.b =>
        collected = res.l +: collected
        if (collected.size == buckets(processBucket).size) {
          pullWorking = false
          pull

          val merged: Seq[Data] = mergeBucketData(collected.flatten, Seq.empty)
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
      case res: GetBucketResp =>
        log.error(s"wrong bucket response, expected=${processBucket}, actual=${res.b}")
        stores.get(self.path.address, "ring_hash").fold(_ ! RestoreState, _ ! RestoreState)
        context.stop(self)
      case DumpIO.PutDone =>
        if (putQueue.isEmpty) {
          if (processBucket == maxBucket) {
            log.info("dump write done")
            stores.get(self.path.address, "ring_hash").fold(_ ! RestoreState, _ ! RestoreState)
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

object DumpIO {
  def props(ioPath: String): Props = Props(new DumpIO(ioPath))

  case object ReadNext
  case class ReadNextRes(kv: Seq[(ByteString, ByteString)], last: Boolean)

  case class Put(kv: Seq[Data])
  case object PutDone
  case object PutClose
}

class DumpIO(ioPath: String) extends Actor with ActorLogging {
  import java.nio.ByteBuffer
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption.{READ, WRITE, CREATE}
  val channel: FileChannel = FileChannel.open(Paths.get(ioPath), READ, WRITE, CREATE)
  def receive = {
    case DumpIO.ReadNext =>
      val key = ByteBuffer.allocateDirect(4)
      val keyRead = channel.read(key)
      if (keyRead == 4) {
        val blockSize: Int = key.flip.asInstanceOf[ByteBuffer].getInt
        val value: Array[Byte] = new Array[Byte](blockSize)
        val valueRead: Int = channel.read(ByteBuffer.wrap(value))
        if (valueRead == blockSize) {
          val kv = DumpKV.parseFrom(value).kv.map(d => d.k -> d.v)
          sender ! DumpIO.ReadNextRes(kv, false)
        } else {
          log.error(s"failed to read dump io, blockSize=${blockSize}, valueRead=${valueRead}")
          sender ! DumpIO.ReadNextRes(Seq.empty, true)
        }
      } else if (keyRead == -1) {
        sender ! DumpIO.ReadNextRes(Seq.empty, true)
      } else {
        log.error(s"failed to read dump io, keyRead=${keyRead}")
        sender ! DumpIO.ReadNextRes(Seq.empty, true)
      }
    case msg: DumpIO.Put => 
      val data = DumpKV(msg.kv.map(e => KV(e.key, e.value))).toByteArray
      channel.write(ByteBuffer.allocateDirect(4).putInt(data.size).flip.asInstanceOf[ByteBuffer])
      channel.write(ByteBuffer.wrap(data))
      sender ! DumpIO.PutDone
    case DumpIO.PutDone => sender ! DumpIO.PutDone
    case DumpIO.PutClose => context.stop(self)
  }
  override def postStop(): Unit = {
    channel.close()
    super.postStop()
  }
}
