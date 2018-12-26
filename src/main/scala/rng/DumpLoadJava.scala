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
      store ! DumpGet(stob("head_of_keys"))
      goto(Collecting) using Some(sender)
  }

  when(Collecting){
    case Event(DumpEn(k,v,nextKey),state) =>
      log.debug("saving state {} -> {}, nextKey = {}", k, v, nextKey)
      if (!nextKey.isEmpty) {
        store ! DumpGet(nextKey)
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
