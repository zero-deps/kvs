package mws.rng

import java.util.concurrent.TimeUnit
import akka.actor._
import akka.util.Timeout
import akka.cluster.Cluster
import scala.concurrent.duration._

class GathererDel(prefList: Set[Node], client: ActorRef) extends FSM[FsmState, Set[Node]] with ActorLogging{
  import context.system

  val config = system.settings.config.getConfig("ring")
  val quorum = config.getIntList("quorum")
  val W: Int = quorum.get(1)
  val local: Address = Cluster(context.system).selfAddress
  setTimer("send_by_timeout", OpsTimeout, config.getInt("gather-timeout").seconds)

  startWith(Collecting, prefList)

  when(Collecting){
    case Event("ok", nodesLeft) =>
      log.info(s"[del] from $prefList , receive response from ${sender.path.address}")
      nodesLeft - addrs(sender()) match {
        case enought if enought.size == W =>
          client ! AckSuccess
          goto(Sent) using(enought)
        case less => stay using(less)
      }
      
    case Event(OpsTimeout, nodesLeft) =>
      //politic of revert is not needed because on read opperation removed data will be saved again,
      //only notify client about failed opperation.
      //deleted on other nodes but we don't know about it ? sorry, eventually consistency
      client ! AckTimeoutFailed
      stop()
  }

  when(Sent){
    case Event("ok", data) =>
      data - addrs(sender())match {
      case none if none.isEmpty => stop()
      case nodes => stay using(nodes)
    }

    case Event(OpsTimeout, nodesLeft) => stop()
  }

  def addrs(s: ActorRef) = (if(s.path.address.hasLocalScope) local else s.path.address) 
  initialize()
}