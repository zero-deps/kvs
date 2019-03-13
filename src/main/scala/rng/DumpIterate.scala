package mws.rng

import akka.actor.{FSM, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import leveldbjnr.{LevelDB}
import mws.kvs.LeveldbOps
import mws.rng.msg_dump.{DumpGet, DumpEn}
import mws.rng.store._
import mws.rng.store.ReadonlyStore
import scala.concurrent.duration._
import scala.concurrent.{Await}
import scala.util.Try

object IterateDumpWorker {

  def props(it: Iterate): Props = Props(new IterateDumpWorker(it))
}
class IterateDumpWorker(it: Iterate) extends FSM[FsmState, Option[ActorRef]] with ActorLogging {
  implicit val timeout = Timeout(120 seconds)
  var dumpDb: LevelDB = _
  var store: ActorRef = _
  val stores = SelectionMemorize(context.system)
  startWith(ReadyCollect, None)

  when(ReadyCollect){
    case Event("go", _) =>
      dumpDb = LeveldbOps.open(context.system, it.path)
      store = context.actorOf(ReadonlyStore.props(dumpDb))
      store ! DumpGet(stob("head_of_keys"))
      goto(Collecting) using Some(sender)
  }

  when(Collecting){
    case Event(DumpEn(k, v, nextKey), state) =>
      val putF: ItPut = (k, v) => {
        Await.ready(stores.get(addr(self), "ring_hash").fold(
          _.ask(InternalPut(k,v)),
          _.ask(InternalPut(k,v)),
        ), timeout.duration)
      }
      it.mapF(k, v, putF)
      if (nextKey.isEmpty) {
        Try(dumpDb.close()).recover{ case err => log.info(s"Error closing db $err")}
        it.afterAllF(putF)
        stores.get(addr(self), "ring_hash").fold(
          _ ! RestoreState,
          _ ! RestoreState,
        )
        state.map(_ ! "done")
        log.info("iteration ended")
        stop()
      } else {
        store ! DumpGet(nextKey)
        stay() using state
      }
  }
}
