package mws.rng

import akka.actor._
import akka.util.Timeout

import scala.annotation.tailrec
import scala.concurrent.duration.DurationDouble

sealed class Gather

case class GatherGet(data: List[(Option[Data], Node)], client: ActorRef) extends Gather

case class GatherPut(statuses: List[String], client: ActorRef) extends Gather

case class GatherDel(statuses: List[String], client: ActorRef) extends Gather

class Gatherer extends Actor with ActorLogging {

  import context.system

  val timeout = Timeout(3 seconds)
  val config = system.settings.config.getConfig("ring")
  val quorum = config.getIntList("quorum")
  val R: Int = quorum.get(1)
  val W: Int = quorum.get(2)

  override def receive: Receive = {
    case GatherGet(data, client) =>
      val value = doGatherGet(data) match {
        case Some(returnData) => Some(returnData.value)
        case None => None
      }
      client ! value

    case GatherPut(statuses, client) =>
      statuses.filter(_.equals("ok")).length match {
        case i: Int if i < W => client ! AckQuorumFailed
        case _ => client ! AckSuccess
      }

    case GatherDel(s, client) =>
      s.count(_.equals("ok"))match {
        case i: Int if i < W => client ! AckQuorumFailed
        case _ => client ! AckSuccess
      }

  }

  def doGatherGet(listData: List[(Option[Data], Node)]): Option[Data] = {
    listData match {
      case l if l isEmpty => None
      case l if l forall  (_._1 == None) => None
      case l if listData.forall(d => d._1 == listData.head._1) => listData.head._1
      case _ =>
        val newest = findLast(listData)
        newest foreach  (updateOutdateNodes(_, listData))
        newest
    }
  }

  def updateOutdateNodes(newData: Data, nodes: List[(Option[Data], Node)]) = {
    nodes.foreach {
      case (d, node) if d.get.vc < newData.vc =>
        val path = RootActorPath(node) / "user" / "ring_store"
        val hs = context.system.actorSelection(path)
        d map (hs ! StorePut(_))
      case _ =>
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
}

  
  
