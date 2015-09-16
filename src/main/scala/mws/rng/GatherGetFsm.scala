package mws.rng

import akka.actor.{ActorRef, RootActorPath, FSM}
import scala.annotation.tailrec
import scala.concurrent.duration._


class GatherGetFsm(clint: ActorRef, R: Int) extends FSM[FsmState, FsmData] {
  
  startWith(Collecting, DataCollection(Nil), Some(2 seconds))
  
  when(Collecting){
    case Event(GetResp(rez), DataCollection(l))  =>
      val head  = (rez, sender().path.address)
      val newData = DataCollection( head :: l)
      newData.l.length match {
        case `R` =>
          doGatherGet(flat(newData.l, Nil))
          stop()
        case _ => 
          stay using newData
      }    
  }

  def doGatherGet(listData: List[(Option[Data], Node)]): Option[Data] = {
    listData match {
      case l if l.isEmpty => None
      case l if l forall  (_._1 == None) => None
      case l if listData.forall(d => d._1 == listData.head._1) => listData.head._1
      case _ =>
        val newest = findLast(listData filter(_._1.isDefined))
        newest foreach  (updateOutdateNodes(_, listData filter(_._1.isDefined)))
        newest
    }
  }

  def findLast(data: List[(Option[Data], Node)]) = {

    @tailrec
    def last(l: List[(Option[Data], Node)], newest: Option[Data]): Option[Data] = l match {
      case (head :: tail) if head._1.get.vc > newest.get.vc => last(tail, head._1)
      case (head :: tail) if
      head._1.get.vc <> newest.get.vc &&
        head._1.get.lastModified > newest.get.lastModified => last(tail, head._1) // last write win if versions are concurrent
      case Nil => newest
      case _ => last(l.tail, newest)
    }

    last(data.tail, data.head._1)
  }

  def updateOutdateNodes(newData: Data, nodes: List[(Option[Data], Node)]) = {
    nodes.foreach {
      case (Some(d), node) if d.vc < newData.vc =>
        val path = RootActorPath(node) / "user" / "ring_store"
        val hs = context.system.actorSelection(path)
        hs ! StorePut(d)
      case _ =>
    }
  }


  def flat(tuples: List[(Option[List[Data]], Node)], res: List[(Option[Data], Node)]): List[(Option[Data], Node)] = {
    tuples match {
      case h :: t => h._1 match {
        case Some(l) =>
          val list: List[(Some[Data], Node)] = l map (d => (Some(d), h._2))
          flat(t, list ++ res)
        case None => flat(t, (None, h._2) :: res)
      }
      case Nil => res
    }
  }
}
