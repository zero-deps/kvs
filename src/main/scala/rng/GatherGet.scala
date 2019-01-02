package mws.rng

import akka.actor._
import akka.cluster.VectorClock
import mws.rng.data.Data
import mws.rng.msg.{StoreGetAck, StorePut, StoreDelete}
import scala.annotation.tailrec
import scala.collection.breakOut
import scala.concurrent.duration._

class GatherGet(client: ActorRef, t: FiniteDuration, M: Int, R: Int, k: Key) extends FSM[FsmState, FsmData] with ActorLogging {
  import GatherGet.{AddrOfData, order}
  val stores = SelectionMemorize(context.system)

  startWith(Collecting, DataCollection(Vector.empty, 0))
  setTimer("send_by_timeout", OpsTimeout, t)

  when(Collecting) {
    case Event(StoreGetAck(rez), DataCollection(perNode, nodes)) =>
      val address = sender.path.address
      val receive: Vector[AddrOfData] =
        if (rez.isEmpty) Vector(None -> address)
        else rez.map(d => Some(d) -> address)(breakOut)

      nodes + 1 match {
        case `M` => //todo: wait for first R same answers?
          cancelTimer("send_by_timeout")
          val (correct: Option[Data], outdated: Vector[Node]) = order(receive ++ perNode);
          { // update outdated nodes with correct data
            val msg = correct.fold[Any](StoreDelete(k))(d => StorePut(Some(d)))
            outdated foreach { node =>
              stores.get(node, "ring_write_store").fold(_ ! msg, _ ! msg)
            }
          }
          //todo: return to client conflict if any
          client ! AckSuccess(correct.map(_.value))
          stop()
        case ns => stay() using DataCollection(receive ++ perNode, ns)
      }

    case Event(OpsTimeout, _) =>
      client ! AckTimeoutFailed
      stop()
  }
}

object GatherGet {
  def props(client: ActorRef, t: FiniteDuration, M: Int, R: Int, k: Key): Props = Props(new GatherGet(client, t, M, R, k))

  type AddrOfData = (Option[Data], Node)

  /* returns (actual data, list of outdated nodes) */
  def order(l: Vector[AddrOfData]): (Option[Data], Vector[Node]) = {
    @tailrec
    def findCorrect(l: Vector[AddrOfData], newest: AddrOfData): AddrOfData = {
      l match {
        case xs if xs.isEmpty => newest
        case h +: t if t.exists(age(h)._1 < age(_)._1) => //todo: remove case (unit test)
          findCorrect(t, newest)
        case h +: t if age(h)._1 > age(newest)._1 =>
          findCorrect(t, h)
        case h +: t if age(h)._1 <> age(newest)._1 && age(h)._2 > age(newest)._2 => 
          findCorrect(t, h)
        case _ =>
          findCorrect(l.tail, newest)
      }
    }
    def age(d: AddrOfData): Age = {
      d._1.fold(new VectorClock -> 0L)(e => makevc(e.vc) -> e.lastModified)
    }
    l.map(age(_)._1).toSet.size match {
      case 0 => None -> Vector.empty
      case 1 => l.head._1 -> Vector.empty
      case n =>
        val correct = findCorrect(l.tail, l.head)
        correct._1 -> l.filterNot(age(_)._1 == age(correct)._1).map(_._2)
    }
  }
}
