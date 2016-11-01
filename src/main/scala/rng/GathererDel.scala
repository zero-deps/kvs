package mws.rng

import akka.actor._
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
      nodesLeft - addrs(sender()) match {
        case enough if prefList.size - enough.size == W => // W nodes removed key
          client ! AckSuccess
          goto(Sent) using(enough)
        case less => stay using(less)
      }
      
    case Event(OpsTimeout, nodesLeft) =>
      //politic of revert is not needed because on read opperation removed data will be saved again,
      //only notify client about failed opperation.
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

  def addrs(s: ActorRef) = if(s.path.address.hasLocalScope) local else s.path.address
  
  initialize()
}