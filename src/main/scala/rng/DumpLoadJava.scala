package zd.rng

import akka.actor.{FSM, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import leveldbjnr.{LevelDb}
import zd.rng.model.{DumpGet, DumpEn}
import zd.rng.store.ReadonlyStore
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

  var dumpDb: LevelDb = _
  var store: ActorRef = _
  val stores = SelectionMemorize(context.system)
  startWith(ReadyCollect, None)

  when(ReadyCollect){
    case Event(DumpProcessor.Load(path),_) =>
      LevelDb.open(path) match {
        case Right(a) => 
          dumpDb = a
          store = context.actorOf(ReadonlyStore.props(dumpDb))
          store ! DumpGet(stob("head_of_keys"))
          goto(Collecting) using Some(sender)
        case Left(t) =>
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
    case Event(x: DumpEn, state) =>
      log.debug("saving state {} -> {}, nextKey = {}", x.k, x.v, x.nextKey)
      size = size + x.v.size
      ksize = ksize + x.k.size
      val putF = stores.get(addr(self), "ring_hash").fold(
        _.ask(InternalPut(x.k, x.v)),
        _.ask(InternalPut(x.k, x.v)),
      )
      Try(Await.result(putF, timeout.duration)) match {
        case Success(_) =>
          if (x.nextKey.isEmpty) {
            stores.get(addr(self), "ring_hash").fold(
              _ ! RestoreState,
              _ ! RestoreState,
            )
            dumpDb.close()
            log.info("load is completed, keys={}", keysNumber)
            state.map(_ ! "done")
            stop()
          } else {
            store ! DumpGet(x.nextKey)
            keysNumber = keysNumber + 1
            if (keysNumber % 10000 == 0) log.info(s"load info: write keys=${keysNumber}, size=${size}, ksize=${ksize}, nextKey=${x.nextKey}")
            stay() using state
          }
        case Failure(t) =>
          log.error(cause=t, message="can't put")
          stores.get(addr(self), "ring_hash").fold(
            _ ! RestoreState,
            _ ! RestoreState,
          )
          dumpDb.close()
          state.map(_ ! "can't put")
          stop()
      }
  }
}
