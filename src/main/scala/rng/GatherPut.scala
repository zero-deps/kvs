package mws.rng

import akka.actor.{ActorLogging, ActorRef, FSM, Props, RootActorPath}
import akka.cluster.VectorClock
import mws.rng.data.Data
import mws.rng.msg.{StoreGetAck, StorePut, StorePutStatus, StorePutSaved}
import scala.concurrent.duration._
import scalaz.Scalaz._

final case class PutInfo(key: Key, v: Value, N: Int, W: Int, bucket: Bucket, localAdr: Node, nodes: Set[Node])

object GatherPut {
  def props(client: ActorRef, t: FiniteDuration, actorsMem: SelectionMemorize, putInfo: PutInfo): Props = Props(new GatherPut(client, t, actorsMem, putInfo))
}

class GatherPut(client: ActorRef, t: FiniteDuration, stores: SelectionMemorize, putInfo: PutInfo) extends FSM[FsmState, Statuses] with ActorLogging {

  startWith(Collecting, Statuses(Vector.empty))
  setTimer("send_by_timeout", OpsTimeout, t)

  when(Collecting){
    case Event(StoreGetAck(data), _) =>
      val vc: VectorClock = if (data.size === 1) {
        makevc(data.head.vc)
      } else if (data.size > 1) {
        data.map(_.vc).foldLeft(new VectorClock)((sum, i) => sum.merge(makevc(i)))
      } else {
        new VectorClock
      }
      val updatedData = Data(putInfo.key, putInfo.bucket, now_ms(), fromvc(vc.:+(putInfo.localAdr.toString)), putInfo.v)
      mapInPut(putInfo.nodes, updatedData)
      stay()
    
    case Event(incomeStatus: StorePutStatus, Statuses(statuses)) =>
      val updStatuses = Statuses(incomeStatus +: statuses)
      updStatuses.all.count(_.isInstanceOf[StorePutSaved]) match {
        case n if n === putInfo.N =>
          client ! AckSuccess(None)
          stop()
        case w if w === putInfo.W =>
          client ! AckSuccess(None)
          goto(Sent) using updStatuses
        case _ => stay using updStatuses
      }

    case Event(OpsTimeout, _) =>
      client ! AckTimeoutFailed
      stop()
  }
  
  // keep fsm running to avoid dead letter warnings
  when(Sent){
    case Event(status: StorePutStatus, Statuses(ss)) =>
      if (ss.size + 1 === putInfo.N) stop()
      else stay() using Statuses(status +: ss)
    case Event(OpsTimeout, _) =>
      stop()
  }

  def mapInPut(nodes: Set[Node], d: Data) = {
    val storeList = nodes.map(n => RootActorPath(n) / "user" / "ring_write_store")
      storeList.foreach(ref => context.system.actorSelection(ref).tell(StorePut(Some(d)), self))
  }
  
  initialize()
}
