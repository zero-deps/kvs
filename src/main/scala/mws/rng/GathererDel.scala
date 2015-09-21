package mws.rng

import akka.actor._
import akka.util.Timeout

import scala.annotation.tailrec
import scala.concurrent.duration.DurationDouble

sealed class Gather

case class GatherGet(data: List[(Option[Data], Node)], client: ActorRef) extends Gather

case class GatherPut(statuses: List[String], client: ActorRef) extends Gather

case class GatherDel(statuses: List[String], client: ActorRef) extends Gather

class GathererDel extends Actor with ActorLogging {
  import context.system

  val timeout = Timeout(5 seconds)
  val config = system.settings.config.getConfig("ring")
  val quorum = config.getIntList("quorum")
  val W: Int = quorum.get(1)

  override def receive: Receive = {

    case GatherDel(s, client) =>
      s.count(_.equals("ok"))match {
        case i: Int if i < W => client ! AckQuorumFailed
        case _ => client ! AckSuccess
      }

  } 
}

  
  
