package mws.rng

import akka.actor.FSM.Normal
import akka.actor._
import scala.annotation.tailrec
import scala.concurrent.duration._


class GatherGetFsm(client: ActorRef, N: Int, R: Int, t: Int)
  extends FSM[FsmState, FsmData] with ActorLogging{
  
  startWith(Collecting, DataCollection(Nil))
  setTimer("send_by_timeout", GatherTimeout, t seconds)
  
  when(Collecting){
    case Event(GetResp(rez), DataCollection(l))  =>
      val head  = (rez, sender().path.address)
      val newData = DataCollection( head :: l)
      newData.l.length match {
        case `R` =>
          val value = doGatherGet(flat(newData.l, Nil)) match {
            case Some(d) => Some(d.value)
            case None => None            
          }
          client ! value
          goto(Sent) using ReceivedValues(R)
        case _ =>
          stay using newData
      }
    case Event(GatherTimeout, DataCollection(l)) =>
      val value = doGatherGet(flat(l, Nil)) match {
        case Some(d) => Some(d.value)
        case None => None
      }
      client ! value  // always readable
      cancelTimer("send_by_timeout")
      stop(Normal)
  } 
  
  when(Sent) {
    case Event(GetResp(rez), ReceivedValues(n)) => {
      if (n + 1 == N)
        stop(Normal)
      else
        stay using ReceivedValues(n + 1)
    }
    case Event(GatherTimeout, _ ) =>
      cancelTimer("send_by_timeout")
      stop(Normal)
  }

  def doGatherGet(listData: List[(Option[Data], Node)]): Option[Data] = {
    listData match {
      case l if l.isEmpty => None
      case l if l forall  (_._1 == None) => None
      case l if listData.forall(d => d._1 == listData.head._1) => listData.head._1
      case _ =>
        val newest = findLast(listData filter(_._1.isDefined))
        newest foreach  (updateOutdatedNodes(_, listData filter(_._1.isDefined)))
        newest
    }
  }

  def findLast(data: List[(Option[Data], Node)]) = {

    @tailrec
    def last(l: List[(Option[Data], Node)], newest: Option[Data]): Option[Data] = l match {
      case Nil => newest
      case (head :: tail) if head._1.get.vc > newest.get.vc => last(tail, head._1)
      case (head :: tail) if
      head._1.get.vc <> newest.get.vc &&
        head._1.get.lastModified > newest.get.lastModified => last(tail, head._1) // last write win if versions are concurrent
      case _ => last(l.tail, newest)
    }

    last(data.tail, data.head._1)
  }

  def updateOutdatedNodes(newData: Data, nodes: List[(Option[Data], Node)]) = {
    nodes.foreach {
      case (Some(d), node) if d.vc < newData.vc =>
        val path = RootActorPath(node) / "user" / "ring_store"
        val hs = context.system.actorSelection(path)
        hs ! StorePut(d)
      case _ =>
    }
  }

  @tailrec
  private def flat(tuples: List[(Option[List[Data]], Node)], res: List[(Option[Data], Node)]): List[(Option[Data], Node)] = tuples match {
    case Nil => res
    case h :: t => h._1 match {
        case Some(l) =>
          val list: List[(Some[Data], Node)] = l map (d => (Some(d), h._2))
          flat(t, list ++ res)
        case None => flat(t, (None, h._2) :: res)
    }
  }


}
