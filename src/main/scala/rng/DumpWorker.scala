package mws.rng

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.google.protobuf.ByteString
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit
import leveldbjnr._
import mws.kvs.store.Ring
import mws.rng.data.Data
import mws.rng.msg.{BucketGet, GetBucketResp, PutSavingEntity, GetSavingEntity, SavingEntity}
import mws.rng.store._
import scala.collection.{SortedMap, SortedSet}
import scala.concurrent.Await
import scala.util.Try
import scalaz.Scalaz._
import scalaz.{Ordering => _, _}

final case class DumpData(current: Bucket, prefList: PreferenceList, collected: Seq[Seq[Data]],
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
                val merged: Seq[Data] = mergeBucketData((data +: state.collected).foldLeft(Seq.empty[Data])((acc, l) => l ++ acc), Nil)
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
                stay() using DumpData(state.current, pList, data +: state.collected, state.lastKey, state.client)

    }

    @scala.annotation.tailrec final def linkKeysInDb(ldata: Seq[Data], prevKey: Option[Key]): Option[Key] = ldata match {
        case Nil => prevKey
        case h::t =>
            log.debug("dump key={}", h.key)
            Await.ready(dumpStore ? PutSavingEntity(h.key, h.value, prevKey.getOrElse(ByteString.EMPTY)), timeout.duration)
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
            store ! GetSavingEntity(stob("head_of_keys"))
            goto(Collecting) using Some(sender)
    }

    when(Collecting){
        case Event(SavingEntity(k,v,nextKey),state) =>
            log.debug("saving state {} -> {}, nextKey = {}", k, v, nextKey)
            val putF = stores.get(self.path.address, "ring_hash").fold(_.ask(InternalPut(k,v)), _.ask(InternalPut(k,v)))
            Await.ready(putF, timeout.duration)
            if (nextKey.isEmpty) {
                stores.get(self.path.address, "ring_hash").fold(_ ! RestoreState, _ ! RestoreState)
                Try(dumpDb.close()).recover{ case err => log.info(s"Error closing db $err")}
                log.info("load is completed, keys={}", keysNumber)
                state.map(_ ! "done")
                stop()
            } else {
                store ! GetSavingEntity(nextKey)
                keysNumber = keysNumber + 1
                stay() using state
            }
    }
}

object IterateDumpWorker {
    def props(path: String, foreach: (ByteString,ByteString)=>Unit): Props = Props(new IterateDumpWorker(path,foreach))
}
class IterateDumpWorker(path: String, foreach: (ByteString,ByteString)=>Unit) extends FSM[FsmState,Option[ActorRef]] with ActorLogging {
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


