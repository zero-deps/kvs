package mws.rng

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.util.Timeout
import scala.concurrent.duration.DurationDouble

case class GatherGet(data: List[(Option[Data], Node)], client: ActorRef)

class Gatherer extends Actor with ActorLogging {
  val timeout = Timeout(3 seconds)

  override def receive: Receive = {
    case GatherGet(data, client) =>
      val value = doGather(data) match {
        case Some(returnData) => Some(returnData._7)
        case None => None
      }
      client ! value
      
  }

  def doGather(listData: List[(Option[Data], Node)]): Option[Data] = {
    if (listData.forall(d => d._1 == listData.head._1)) {
      listData.head._1
    } else {
      findLast(listData)
    }
  }

  def findLast(data: List[(Option[Data], Node)]) = {
    def last(l: List[(Option[Data], Node)], newest: Option[Data]): Option[Data] = l match {
      case (head :: tail) if head._1.get._4 > newest.get._4 => last(tail, head._1)
      case (head :: tail) if head._1.get._4 <> newest.get._4 && head._1.get._3 > newest.get._3 => last(tail, head._1) // last write win
      case Nil => newest
    }
    last(data.tail, data.head._1)
  }
}

  
  
