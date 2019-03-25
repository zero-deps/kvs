package mws.rng

import akka.actor.{FSM, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import leveldbjnr.{LevelDB}
import mws.kvs.LeveldbOps
import mws.rng.model.{DumpGet, DumpEn}
import mws.rng.store.ReadonlyStore
import scala.concurrent.duration._
import scala.concurrent.{Await}
import scala.util.{Try, Success, Failure}

object LoadDumpWorkerJava {
  def props(): Props = Props(new LoadDumpWorkerJava)
}

class LoadDumpWorkerJava extends FSM[FsmState, Option[ActorRef]] with ActorLogging {
  implicit val timeout = Timeout(120 seconds)
  var keysNumber = 0
  var size: Long = 0L
  var ksize: Long = 0L

  var dumpDb: LevelDB = _
  var store: ActorRef = _
  val stores = SelectionMemorize(context.system)
  startWith(ReadyCollect, None)

  when(ReadyCollect){
    case Event(DumpProcessor.Load(path),_) =>
      Try(LeveldbOps.open(path)) match {
        case Success(a) => 
          dumpDb = a
          store = context.actorOf(ReadonlyStore.props(dumpDb))
          store ! DumpGet(stob("head_of_keys"))
          goto(Collecting) using Some(sender)
        case Failure(t) =>
          stores.get(addr(self), "ring_hash").fold(
            _ ! RestoreState,
            _ ! RestoreState,
          )
          sender ! "invalid path"
          log.error(cause=t, message=s"Invalid path of dump=${path}")
          stop()
      }
  }

  when(Collecting){
    case Event(DumpEn(k,v,nextKey),state) =>
      log.debug("saving state {} -> {}, nextKey = {}", k, v, nextKey)
      size = size + v.size
      ksize = ksize + k.size
      val putF = stores.get(addr(self), "ring_hash").fold(
        _.ask(InternalPut(k,v)),
        _.ask(InternalPut(k,v)),
      )
      Try(Await.result(putF, timeout.duration)) match {
        case Success(_) =>
          if (nextKey.isEmpty) {
            stores.get(addr(self), "ring_hash").fold(
              _ ! RestoreState,
              _ ! RestoreState,
            )
            Try(dumpDb.close()).recover{ case err => log.info(s"Error closing db $err")}
            log.info("load is completed, keys={}", keysNumber)
            state.map(_ ! "done")
            stop()
          } else {
            store ! DumpGet(nextKey)
            keysNumber = keysNumber + 1
            if (keysNumber % 10000 == 0) log.info(s"load info: write keys=${keysNumber}, size=${size}, ksize=${ksize}, nextKey=${nextKey}")
            stay() using state
          }
        case Failure(t) =>
          log.error(cause=t, message="can't put")
          stores.get(addr(self), "ring_hash").fold(
            _ ! RestoreState,
            _ ! RestoreState,
          )
          Try(dumpDb.close()).recover{ case err => log.info(s"Error closing db $err")}
          state.map(_ ! "can't put")
          stop()
      }
  }
}
