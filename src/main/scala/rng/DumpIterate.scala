package mws.rng

import akka.actor.{FSM, ActorRef, ActorLogging, Props}
import leveldbjnr._
import mws.kvs.LeveldbOps
import mws.rng.model.{DumpGet, DumpEn}
import mws.rng.store._
import scala.util.{Try, Success, Failure}

object IterateDumpWorker {
  def props(path: String, f: (Key,Value) => Unit): Props = Props(new IterateDumpWorker(path, f))
}
class IterateDumpWorker(path: String, f: (Key,Value) => Unit) extends FSM[FsmState, Option[ActorRef]] with ActorLogging {
  var dumpDb: LevelDB = _
  var store: ActorRef = _
  val stores = SelectionMemorize(context.system)
  startWith(ReadyCollect, None)

  when(ReadyCollect){
    case Event(Iterate(_,_),_) =>
      Try(LeveldbOps.open(path)) match {
        case Success(a) =>
          dumpDb = a
          store = context.actorOf(ReadonlyStore.props(dumpDb))
          store ! DumpGet(stob("head_of_keys"))
          goto(Collecting) using Some(sender)
        case Failure(t) =>
          sender ! "done"
          log.error(cause=t, message=s"Invalid path of dump=${path}")
          stop()
      }
  }

  when(Collecting){
    case Event(DumpEn(k,v,nextKey),state) =>
      log.debug(s"iterate key=$k")
      f(k,v)
      if (nextKey.isEmpty) {
        Try(dumpDb.close()).recover{ case err => log.info(s"Error closing db $err")}
        state.map(_ ! "done")
        log.info("iteration ended")
        stop()
      } else {
        store ! DumpGet(nextKey)
        stay() using state
      }
  }
}
