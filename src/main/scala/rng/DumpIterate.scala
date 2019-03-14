package mws.rng

import akka.actor.{FSM, ActorRef, ActorLogging, Props}
import com.google.protobuf.{ByteString}
import leveldbjnr._
import mws.kvs.LeveldbOps
import mws.rng.msg_dump.{DumpGet, DumpEn}
import mws.rng.store._
import scala.util.Try

object IterateDumpWorker {
  def props(path: String, f: (ByteString,ByteString) => Unit): Props = Props(new IterateDumpWorker(path, f))
}
class IterateDumpWorker(path: String, f: (ByteString,ByteString) => Unit) extends FSM[FsmState, Option[ActorRef]] with ActorLogging {
  var dumpDb: LevelDB = _
  var store: ActorRef = _
  val stores = SelectionMemorize(context.system)
  startWith(ReadyCollect, None)

  when(ReadyCollect){
    case Event(Iterate(_,_),_) =>
      dumpDb = LeveldbOps.open(context.system, path)
      store = context.actorOf(Props(classOf[ReadonlyStore], dumpDb))
      store ! DumpGet(stob("head_of_keys"))
      goto(Collecting) using Some(sender)
  }

  when(Collecting){
    case Event(DumpEn(k,v,nextKey),state) =>
      log.debug(s"iterate key=$k")
      f(k,v)
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
