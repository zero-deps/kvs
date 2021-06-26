package zd.rng

import annotation.unused
import akka.actor.*
import zd.rng.data.Data
import zd.rng.model.{StoreGetAck, StorePut, StoreDelete}
import scala.concurrent.duration.*
import scala.collection.immutable.{HashSet}

import GatherGet.DataCollection

class GatherGet(client: ActorRef, t: FiniteDuration, M: Int, @unused R: Int, k: Key) extends FSM[FsmState, DataCollection] with ActorLogging {
  val stores = SelectionMemorize(context.system)

  startWith(Collecting, DataCollection(Vector.empty, 0))
  setTimer("send_by_timeout", "timeout", t)

  when(Collecting) {
    case Event(StoreGetAck(data), DataCollection(perNode, nodes)) =>
      val xs = (data -> addr(sender)) +: perNode
      nodes + 1 match {
        case `M` => //todo; wait for first R same answers?
          cancelTimer("send_by_timeout")
          val (correct: Option[Data], outdated: HashSet[Node]) = MergeOps.forGatherGet(xs)
          ;{ // update outdated nodes with correct data
            val msg = correct.fold[Any](StoreDelete(k))(d => StorePut(d))
            outdated foreach { node =>
              stores.get(node, "ring_write_store").fold(_ ! msg, _ ! msg)
            }
          }
          client ! AckSuccess(correct.map(_.value))
          stop()
        case ns =>
          stay using DataCollection(xs, ns)
      }

    case Event("timeout", _) =>
      client ! AckTimeoutFailed("get", new String(k, "UTF-8"))
      stop()
  }
}

object GatherGet {
  def props(client: ActorRef, t: FiniteDuration, M: Int, R: Int, k: Key): Props = Props(new GatherGet(client, t, M, R, k))

  type AddrOfData = (Option[Data], Node)

  final case class DataCollection(perNode: Vector[AddrOfData], nodes: Int)
}
