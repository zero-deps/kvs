package mws.rng

import akka.actor.{FSM, ActorRef, ActorLogging, Props}
import akka.pattern.ask
import akka.util.Timeout
import leveldbjnr._
import mws.kvs.LeveldbOps
import mws.rng.model.{DumpGet, DumpEn}
import mws.rng.store._
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.{Try, Success, Failure}

object IterateDumpWorker {
  def props(path: String, f: (Key,Value) => Option[(Key, Value)]): Props = Props(new IterateDumpWorker(path, f))
}

class IterateDumpWorker(path: String, f: (Key, Value) => Option[(Key, Value)]) extends FSM[FsmState, Option[ActorRef]] with ActorLogging {
  implicit val timeout = Timeout(120 seconds)
  var keysNumber = 0
  var size: Long = 0L
  var ksize: Long = 0L

  var dumpDb: LevelDB = _
  var store: ActorRef = _
  val stores = SelectionMemorize(context.system)
  startWith(ReadyCollect, None)

  def restoreState: Unit =
    stores.get(addr(self), "ring_hash").fold(
      _ ! RestoreState,
      _ ! RestoreState,
    )

  when(ReadyCollect){
    case Event(Iterate(_,_),_) =>
      Try(LeveldbOps.open(path)) match {
        case Success(a) =>
          dumpDb = a
          store = context.actorOf(ReadonlyStore.props(dumpDb))
          store ! DumpGet(stob("head_of_keys"))
          goto(Collecting) using Some(sender)
        case Failure(t) =>
          log.error(cause=t, message=s"Invalid path of dump=${path}")
          sender ! "done"
          restoreState
          stop()
      }
  }

  when(Collecting){
    case Event(DumpEn(k,v,nextKey),state) =>
      log.debug("iterate state {} -> {}, nextKey = {}", k, v, nextKey)
      size = size + v.size
      ksize = ksize + k.size

      def next = 
        if (nextKey.isEmpty) {
          log.info("load is completed, keys={}, size={}, ksize={}", keysNumber, size, ksize)
          Try(dumpDb.close()).recover{ case err => log.info(s"Error closing db $err")}
          state.map(_ ! "done")
          restoreState
          stop()
        } else {
          store ! DumpGet(nextKey)
          keysNumber = keysNumber + 1
          if (keysNumber % 10000 == 0) log.info("iteration info: write keys=${}, size={}, ksize={}", keysNumber, size, ksize)
          stay() using state
        }

      f(k,v) match {
        case Some((newK, newV)) =>
          val putF = stores.get(addr(self), "ring_hash").fold(
            _.ask(InternalPut(newK, newV)),
            _.ask(InternalPut(newK, newV)),
          )
          Try(Await.ready(putF, timeout.duration)) match {
            case Success(a) =>
              log.debug("iterate update {} to {} -> {}", k, newK, newV)
              next
            case Failure(err) =>
              log.error("stop iterate. failed to put", err)
              restoreState
              stop()
          }
        case None =>
          log.debug("skip {}", k)
          next
      }
  }
}
