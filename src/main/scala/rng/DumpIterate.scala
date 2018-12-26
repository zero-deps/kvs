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
      store ! DumpGet(stob("head_of_keys"))
      goto(Collecting) using Some(sender)
  }

  when(Collecting){
    case Event(DumpEn(k,v,nextKey),state) =>
      log.debug(s"iterate key=$k")
      foreach(k,v)
      if (nextKey.isEmpty) {
        Try(dumpDb.close()).recover{ case err => log.info(s"Error closing db $err")}
        state.map(_ ! "done")
        log.debug("iteration ended")
        stop()
      } else {
        store ! DumpGet(nextKey)
        stay() using state
      }
  }
}
