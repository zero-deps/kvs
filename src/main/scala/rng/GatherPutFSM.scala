package mws.rng

import akka.actor.{ActorLogging, ActorRef, FSM, Props, RootActorPath}
import akka.cluster.VectorClock
import mws.rng.data.Data
import mws.rng.msg.{GetResp, StorePut}
import mws.rng.store.{PutStatus, Saved}
import scala.concurrent.duration._
import scalaz.Scalaz._

case class PutInfo(key: Key, v: Value, N: Int, W: Int, bucket: Bucket, localAdr: Node, nodes: Set[Node])
case class PutInfoBulk(b: List[(Key, Value)], N: Int, W: Int, bucket: Bucket, localAdr: Node, nodes: Set[Node])

object GatherPutFSM {
  def props(client: ActorRef, t: Int, actorsMem: SelectionMemorize, putInfo: PutInfo): Props = Props(new GatherPutFSM(client, t, actorsMem, putInfo))
}

class GatherPutFSM(client: ActorRef, t: Int, stores: SelectionMemorize, putInfo: PutInfo)
  extends FSM[FsmState, FsmData] with ActorLogging {

  startWith(Collecting, Statuses(Nil))
  setTimer("send_by_timeout", OpsTimeout, t.seconds)

  when(Collecting){
    case Event(GetResp(data), _) =>
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
    
    case Event(incomeStatus: PutStatus, Statuses(statuses)) =>
      val updStatuses = Statuses( incomeStatus :: statuses )
      updStatuses.all.count(_ === Saved) match {
        case n if n == putInfo.N =>
          client ! AckSuccess
          stop()
        case w if w == putInfo.W =>
          client ! AckSuccess
          goto(Sent) using updStatuses
        case _ => stay using updStatuses
      }

    case Event(OpsTimeout, _) =>
      client ! AckTimeoutFailed
      cancelTimer("send_by_timeout")
      stop()
  }
  
  when(Sent){
    case Event(status: PutStatus, Statuses(ss)) =>
      if(ss.size + 1 == putInfo.N)
        stop()
      else
        stay using Statuses(status :: ss)
    case Event(OpsTimeout, _ ) =>
      cancelTimer("send_by_timeout")
      stop()
  }

  def mapInPut(nodes: Set[Node], d: Data) = {
    val storeList = nodes.map(n => RootActorPath(n) / "user" / "ring_write_store")
      storeList.foreach(ref => context.system.actorSelection(ref).tell(StorePut(Some(d)), self))
  }
  
  initialize()
}
