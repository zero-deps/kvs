package mws.rng

import akka.actor.{ActorRef, Actor, ActorLogging}
import akka.cluster.VectorClock
import akka.util.Timeout

import scala.annotation.tailrec
import scala.concurrent.duration.DurationDouble

case class GatherGet(data: List[(List[Data], Node)], client: ActorRef)

class Gatherer extends Actor with ActorLogging {
  val timeout = Timeout(3 seconds)

  override def receive: Receive = {
    case GatherGet(data, client) =>
      

  }

  @tailrec
  private def gatherNodesGet(toMerge: List[List[Data]], merged: List[Data]): List[Data] = toMerge match {
    case Nil => merged
    case (head :: tail) => gatherNodesGet(tail, head ++ merged)
  }

  @tailrec
  private def mergeGetResult(data: List[Data], uniqueData: List[Data]): List[Data] = data match {
    case (head :: tail) => {
      val vc: VectorClock = head._4
      tail filter (d => d._4 > vc) size match {
        case 0 => {
          uniqueData filter (d => d._4 > vc || d._4 == vc) size match {
            case 0 => mergeGetResult(tail, head :: uniqueData)
            case _ => mergeGetResult(tail, uniqueData)
          }
        }
        case _ => mergeGetResult(tail, uniqueData)
      }
    }
    case _ => uniqueData
  }

}
