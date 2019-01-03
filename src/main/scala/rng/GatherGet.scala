package mws.rng

import akka.actor._
import akka.cluster.VectorClock
import mws.rng.data.Data
import mws.rng.msg.{StoreGetAck, StorePut, StoreDelete}
import scala.annotation.tailrec
import scala.concurrent.duration._

import GatherGet.DataCollection

class GatherGet(client: ActorRef, t: FiniteDuration, M: Int, R: Int, k: Key) extends FSM[FsmState, DataCollection] with ActorLogging {
  import GatherGet.{order}
  val stores = SelectionMemorize(context.system)

  startWith(Collecting, DataCollection(Vector.empty, 0))
  setTimer("send_by_timeout", OpsTimeout, t)

  when(Collecting) {
    case Event(StoreGetAck(data), DataCollection(perNode, nodes)) =>
      val xs = (data -> addr(sender)) +: perNode
      nodes + 1 match {
        case `M` => //todo: wait for first R same answers?
          cancelTimer("send_by_timeout")
          val (correct: Option[Data], outdated: Vector[Node]) = order(xs)
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

    case Event(OpsTimeout, _) =>
      client ! AckTimeoutFailed
      stop()
  }
}

object GatherGet {
  def props(client: ActorRef, t: FiniteDuration, M: Int, R: Int, k: Key): Props = Props(new GatherGet(client, t, M, R, k))

  type AddrOfData = (Option[Data], Node)

  final case class DataCollection(perNode: Vector[AddrOfData], nodes: Int)

  /* returns (actual data, list of outdated nodes) */
  def order(xs: Vector[AddrOfData]): (Option[Data], Vector[Node]) = {
    @tailrec
    def findCorrect(xs: Vector[AddrOfData], newest: AddrOfData): AddrOfData = {
      xs match {
        case xs if xs.isEmpty => newest
        case h +: t if t.exists(age1(h) < age1(_)) => //todo: remove case (unit test)
          findCorrect(t, newest)
        case h +: t if age1(h) > age1(newest) =>
          findCorrect(t, h)
        case h +: t if age1(h) <> age1(newest) && age2(h) > age2(newest) => 
          findCorrect(t, h)
        case xs =>
          findCorrect(xs.tail, newest)
      }
    }
    def age1(d: AddrOfData): VectorClock = {
      d._1.fold(new VectorClock)(e => makevc(e.vc))
    }
    def age2(d: AddrOfData): Long = {
      d._1.fold(0L)(_.lastModified)
    }
    xs.map(age1(_)).toSet.size match {
      case 0 => None -> Vector.empty
      case 1 => xs.head._1 -> Vector.empty
      case n =>
        val correct = findCorrect(xs.tail, xs.head)
        correct._1 -> xs.filterNot(age1(_) == age1(correct)).map(_._2)
    }
  }
}
