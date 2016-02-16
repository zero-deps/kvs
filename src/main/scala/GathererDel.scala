package mws.rng

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.util.Timeout

sealed class Gather
case class GatherDel(statuses: List[String], client: ActorRef) extends Gather

class GathererDel extends Actor with ActorLogging {
  import context.system

  val timeout = Timeout(5, TimeUnit.SECONDS)
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