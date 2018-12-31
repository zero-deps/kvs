package mws.rng

import akka.actor._
import akka.cluster.VectorClock
import mws.rng.data.Data
import mws.rng.msg.{StoreGetAck, StorePut, StoreDelete}
import scala.annotation.tailrec
import scala.collection.breakOut
import scala.concurrent.duration._

object GatherGet {
  def props(client: ActorRef, t: FiniteDuration, M: Int, R: Int, k: Key): Props = Props(new GatherGet(client, t, M, R, k))
}

class GatherGet(client: ActorRef, t: FiniteDuration, M: Int, R: Int, k: Key) extends FSM[FsmState, FsmData] with ActorLogging {
  val stores = SelectionMemorize(context.system)

  startWith(Collecting, DataCollection(Vector.empty, 0))
  setTimer("send_by_timeout", OpsTimeout, t)

  type AddrOfData = (Option[Data], Node)

  when(Collecting) {
    case Event(StoreGetAck(rez), DataCollection(perNode, nodes)) =>
      val address = sender.path.address
      val receive: Vector[AddrOfData] =
        if (rez.isEmpty) Vector(None -> address)
        else rez.map(d => Some(d) -> address)(breakOut)

      nodes + 1 match {
        case `M` => // TODO wait for R or M nodes ?
          cancelTimer("send_by_timeout")
          val (correct, outdated) = order(receive ++ perNode)
          if (outdated.nonEmpty) {
            correct.foreach(freshData => update(freshData._1, outdated.map(_._2)))
          }
          client ! AckSuccess(correct.fold[Option[Value]](None)(d => d._1.fold[Option[Value]](None)(v => Some(v.value))))
          stop()
        case ns => stay() using DataCollection(receive ++ perNode, ns)
      }

    case Event(OpsTimeout, _) =>
      client ! AckTimeoutFailed
      stop()
  }

  /* returns (actual data, list of outdated nodes) */
  def order(l: Vector[AddrOfData]): (Option[AddrOfData], Vector[AddrOfData]) = {
    @tailrec
    def findCorrect(l: Vector[AddrOfData], newest: AddrOfData): AddrOfData = l match {
      case xs if xs.isEmpty => newest
      case h +: t if t.exists(age(h)._1 < age(_)._1) =>
        findCorrect(t, newest)
      case h +: t if age(h)._1 > age(newest)._1 =>
        findCorrect(t, h)
      case h +: t if age(h)._1 <> age(newest)._1 && age(h)._2 > age(newest)._2 => 
        findCorrect(t, h)
      case _ =>
        findCorrect(l.tail, newest)
    }
    def age(d: AddrOfData): Age = {
      d._1.fold((new VectorClock, 0L))(e => (makevc(e.vc), e.lastModified))
    }
    l.map(age(_)._1).toSet.size match {
      case 0 => None -> Vector.empty
      case 1 => Some(l.head) -> Vector.empty
      case n =>
        val correct = findCorrect(l.tail, l.head)
        Some(correct) -> l.filterNot(age(_)._1 == age(correct)._1)
    }
  }

  def update(correct: Option[Data], nodes: Seq[Node]): Unit = {
    val msg = correct match {
      case Some(d) => StorePut(Some(d))
      case None => StoreDelete(k)
    }
    nodes foreach {
      stores.get(_, "ring_write_store").fold(_ ! msg, _ ! msg)
    }
  }
}
