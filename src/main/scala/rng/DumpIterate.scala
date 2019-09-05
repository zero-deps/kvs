package zd.kvs
package rng

import akka.actor.{FSM, ActorRef, ActorLogging, Props}
import akka.pattern.ask
import akka.util.Timeout
import leveldbjnr._
import zd.kvs.rng.model.{DumpGet, DumpEn}
import zd.kvs.rng.store._
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.{Try, Success, Failure}

object IterateDumpWorker {
  def props(path: String, f: (Key,Value) => Option[(Key, Value)], afterIterate: () => Unit): Props = Props(new IterateDumpWorker(path, f, afterIterate))
}

class IterateDumpWorker(path: String, f: (Key, Value) => Option[(Key, Value)], afterIterate: () => Unit) extends FSM[FsmState, Option[ActorRef]] with ActorLogging {
  implicit val timeout = Timeout(120 seconds)
  var keysNumber = 0
  var size: Long = 0L
  var ksize: Long = 0L

  var dumpDb: LevelDb = _
  var store: ActorRef = _
  val stores = SelectionMemorize(context.system)
  startWith(ReadyCollect, None)

  def restoreState(): Unit =
    stores.get(addr(self), "ring_hash").fold(
      _ ! RestoreState,
      _ ! RestoreState,
    )

  when(ReadyCollect){
    case Event(Iterate(_,_,_),_) =>
      LevelDb.open(path) match {
        case Right(a) =>
          dumpDb = a
          store = context.actorOf(ReadonlyStore.props(dumpDb))
          store ! DumpGet(stob("head_of_keys"))
          goto(Collecting) using Some(sender)
        case Left(t) =>
          log.error(cause=t, message=s"Invalid path of dump=${path}")
          sender ! "invalid path"
          restoreState()
          stop()
      }
  }

  when(Collecting){
    case Event(x: DumpEn, state) =>
      log.debug("iterate state {} -> {}, nextKey = {}", x.k, x.v, x.nextKey)
      size = size + x.v.size
      ksize = ksize + x.k.size

      def next = 
        if (x.nextKey.isEmpty) {
          log.info("load is completed, keys={}, size={}, ksize={}", keysNumber, size, ksize)
          dumpDb.close()
          state.map(_ ! "done")
          restoreState()
          afterIterate()
          stop()
        } else {
          store ! DumpGet(x.nextKey)
          keysNumber = keysNumber + 1
          if (keysNumber % 10000 == 0) log.info("iteration info: write keys=${}, size={}, ksize={}", keysNumber, size, ksize)
          stay() using state
        }

      f(x.k, x.v) match {
        case Some((newK, newV)) =>
          val putF = stores.get(addr(self), "ring_hash").fold(
            _.ask(InternalPut(newK, newV)),
            _.ask(InternalPut(newK, newV)),
          )
          Try(Await.result(putF, timeout.duration)) match {
            case Success(_) =>
              log.debug("iterate update {} to {} -> {}", x.k, newK, newV)
              next
            case Failure(err) =>
              log.error("stop iterate. failed to put", err)
              dumpDb.close()
              state.map(_ ! "failed to put")
              restoreState()
              afterIterate()
              stop()
          }
        case None =>
          log.debug("skip {}", x.k)
          next
      }
  }
}
