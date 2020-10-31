package kvs
package rng

import akka.actor._
import akka.cluster.Cluster
import scala.concurrent.duration._
import zd.proto.Bytes

class GatherDel(client: ActorRef, t: FiniteDuration, prefList: Set[Node], k: Bytes, conf: RngConf) extends FSM[FsmState, Set[Node]] with ActorLogging {
  val quorum = conf.quorum
  val W: Int = quorum.W
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
      /* politic of revert is not needed because on read opperation removed data will be saved again,
       * only notify client about failed opperation.
       * deleted on other nodes but we don't know about it ? sorry, eventually consistency
       */
      client ! AckTimeoutFailed("del", k)
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
  def props(client: ActorRef, t: FiniteDuration, prefList: Set[Node], k: Bytes, conf: RngConf): Props = Props(new GatherDel(client, t, prefList, k, conf))
}
