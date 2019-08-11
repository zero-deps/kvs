package zd.rng

import akka.actor._
import akka.cluster.Cluster
import scala.concurrent.duration._

class GatherDel(client: ActorRef, t: FiniteDuration, prefList: Set[Node], k: Key) extends FSM[FsmState, Set[Node]] with ActorLogging {
  import context.system

  val config = system.settings.config.getConfig("ring")
  val quorum = config.getIntList("quorum")
  val W: Int = quorum.get(1)
  val local: Address = Cluster(context.system).selfAddress
  setTimer("send_by_timeout", "timeout", t)

  startWith(Collecting, prefList)

  when(Collecting){
    case Event("ok", nodesLeft) =>
      nodesLeft - addr1(sender) match {
        case enough if prefList.size - enough.size == W => // W nodes removed key
          client ! AckSuccess(None)
          goto(Sent) using(enough)
        case less => stay using(less)
      }
      
    case Event("timeout", nodesLeft) =>
      //politic of revert is not needed because on read opperation removed data will be saved again,
      //only notify client about failed opperation.
      //deleted on other nodes but we don't know about it ? sorry, eventually consistency
      client ! AckTimeoutFailed(s"del=${k}")
      stop()
  }

  when(Sent){
    case Event("ok", data) =>
      data - addr1(sender) match {
      case none if none.isEmpty => stop()
      case nodes => stay using(nodes)
    }

    case Event("timeout", _) => stop()
  }

  def addr1(s: ActorRef) = if (addr(s).hasLocalScope) local else addr(s)
  
  initialize()
}

object GatherDel {
  def props(client: ActorRef, t: FiniteDuration, prefList: Set[Node], k: Key): Props = Props(new GatherDel(client, t, prefList, k))
}
