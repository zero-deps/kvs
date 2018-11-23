package mws.rng

import akka.actor._
import mws.kvs.store.Ring
import mws.rng.store._
import akka.util.{ByteString, Timeout}
import java.util.Calendar

import akka.pattern.ask

import scala.collection.{SortedMap, SortedSet}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import leveldbjnr._

import scalaz.{Ordering => _, _}
import Scalaz._
import scala.concurrent.Await
import scala.util.Try

final case class DumpData(current: Bucket, prefList: PreferenceList, collected: List[List[Data]],
                          lastKey: Option[Key], client: Option[ActorRef])

object DumpWorker {
    def props(buckets: SortedMap[Bucket, PreferenceList], local: Node, path: String): Props = Props(new DumpWorker(buckets, local, path))
}
class DumpWorker(buckets: SortedMap[Bucket, PreferenceList], local: Node, path: String) extends FSM[FsmState, DumpData] with ActorLogging {
    implicit val ord = Ordering.by[Node, String](n => n.hostPort)

    val timestamp = new SimpleDateFormat("yyyy.MM.dd-HH.mm.ss").format(Calendar.getInstance().getTime)
    val filePath = s"$path/rng_dump_$timestamp"
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

    startWith(ReadyCollect, DumpData(0, SortedSet.empty[Node], Nil, None, None))

    when(ReadyCollect){
        case Event(Dump(_), state ) =>
            db = Ring.openLeveldb(context.system, filePath.some)
            dumpStore = context.actorOf(Props(classOf[WriteStore], db))
            buckets(state.current).foreach{n => stores.get(n, "ring_readonly_store").fold(_ ! BucketGet(state.current), _ ! BucketGet(state.current))}
            goto(Collecting) using DumpData(state.current, buckets(state.current), Nil, None, Some(sender))
    }

    when(Collecting){
        case Event(GetBucketResp(b,data), state) => // TODO add timeout if any node is not responding.
            val pList = state.prefList - (if(sender().path.address.hasLocalScope) local else sender().path.address)
            if (pList.isEmpty) {
                val lastKey = mergeBucketData((data :: state.collected).foldLeft(List.empty[Data])((acc, l) => l ::: acc), Nil) match {
                  case Nil => state.lastKey
                  case listData =>  linkKeysInDb(listData, state.lastKey)
                }
                b+1 match {
                    case `maxBucket` =>
                        stores.get(self.path.address, "ring_hash").fold(_ ! RestoreState, _ ! RestoreState)
                        log.info(s"Dump complete, sending path to hash, lastKey = $lastKey, keysInDump=$keysInDump")
                        dumpStore ! PutSavingEntity("head_of_keys", (ByteString("dummy"), lastKey))
                        dumpStore ! PoisonPill //TODO stop after processing last msg
                        import mws.rng.arch.Archiver._
                        Thread.sleep(5000)
                        zip(filePath)
                        log.info(s"zip dump ok=$filePath")
                        state.client.map(_ ! s"$filePath.zip")
                        Try(db.close()).recover{ case err => log.info(s"Error closing db $err")}
                        stop()
                    case nextBucket =>
                        buckets(nextBucket).foreach{n => stores.get(n, "ring_readonly_store").fold(_ ! BucketGet(nextBucket), _ ! BucketGet(nextBucket))}
                        stay() using DumpData(nextBucket, buckets(nextBucket), Nil, lastKey, state.client)
                }
            }
            else
                stay() using DumpData(state.current, pList, data :: state.collected, state.lastKey, state.client)

    }

    @scala.annotation.tailrec final def linkKeysInDb(ldata: List[Data], prevKey: Option[Key]): Option[Key] = ldata match {
        case Nil => prevKey
        case h::t =>
            log.debug("dump key={}", h.key)
            Await.ready(dumpStore ? PutSavingEntity(h.key, (h.value, prevKey)), timeout.duration)
            keysInDump = keysInDump + 1
            linkKeysInDb(t,Some(h.key))
    }

    initialize()
}

object LoadDumpWorker {
    def props(path: String): Props = Props(new LoadDumpWorker(path))
}
class LoadDumpWorker(path: String) extends FSM[FsmState, Option[ActorRef]] with ActorLogging {
    import mws.rng.arch.Archiver._
    implicit val timeout = Timeout(120, TimeUnit.SECONDS)
    var keysNumber = 0

    val extraxtedDir = path.dropRight(".zip".length)
    unZipIt(path, extraxtedDir)
    var dumpDb: LevelDB = _
    var store: ActorRef = _
    val stores = SelectionMemorize(context.system)
    startWith(ReadyCollect, None)

    when(ReadyCollect){
        case Event(LoadDump(_),_) =>
            dumpDb = Ring.openLeveldb(context.system, extraxtedDir.some)
            store = context.actorOf(Props(classOf[ReadonlyStore], dumpDb))
            store ! GetSavingEntity("head_of_keys")
            goto(Collecting) using Some(sender)
    }

    when(Collecting){
        case Event(SavingEntity(k,v,nextKey),state) =>
            log.debug("saving state {} -> {}, nextKey = {}", k, v, nextKey)
            val putF = stores.get(self.path.address, "ring_hash").fold(_.ask(InternalPut(k,v)), _.ask(InternalPut(k,v)))
            Await.ready(putF, timeout.duration)
            nextKey match {
                case None =>
                    stores.get(self.path.address, "ring_hash").fold(_ ! RestoreState, _ ! RestoreState)
                    Try(dumpDb.close()).recover{ case err => log.info(s"Error closing db $err")}
                    log.info("load is completed, keys={}", keysNumber)
                    state.map(_ ! "done")
                    stop()
                case Some(key) =>
                    store ! GetSavingEntity(key)
                    keysNumber = keysNumber + 1
                    stay() using state
            }
    }
}

object IterateDumpWorker {
    def props(path: String, foreach: (String,Array[Byte])=>Unit): Props = Props(new IterateDumpWorker(path,foreach))
}
class IterateDumpWorker(path: String, foreach: (String,Array[Byte])=>Unit) extends FSM[FsmState,Option[ActorRef]] with ActorLogging {
    import mws.rng.arch.Archiver._
    val extraxtedDir = path.dropRight(".zip".length)

    unZipIt(path, extraxtedDir)
    var dumpDb: LevelDB = _
    var store: ActorRef = _
    val stores = SelectionMemorize(context.system)
    startWith(ReadyCollect, None)

    when(ReadyCollect){
        case Event(IterateDump(_,_),_) =>
            dumpDb = Ring.openLeveldb(context.system,extraxtedDir.some)
            store = context.actorOf(Props(classOf[ReadonlyStore], dumpDb))
            store ! GetSavingEntity("head_of_keys")
            goto(Collecting) using Some(sender)
    }

    when(Collecting){
        case Event(SavingEntity(k,v,nextKey),state) =>
            log.debug(s"iterate key=$k")
            foreach(k,v.toArray)
            nextKey match {
                case None =>
                    Try(dumpDb.close()).recover{ case err => log.info(s"Error closing db $err")}
                    state.map(_ ! "done")
                    log.debug("iteration ended")
                    stop()
                case Some(key) =>
                    store ! GetSavingEntity(key)
                    stay() using state
            }
    }
}
