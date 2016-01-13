package mws.rng

import akka.actor.{Props, ActorLogging, ActorRef, FSM}
import akka.cluster.VectorClock
import scala.concurrent.duration._

case class PutInfo(key: Key, v: Value, N: Int, W: Int, bucket: Bucket, localHashAdr: Node, nodes: List[Node])

object GatherPutFSM{
  def props(client: ActorRef, t: Int, actorsMem: SelectionMemorize, putInfo: PutInfo) = Props(
    classOf[GatherPutFSM], client, t, actorsMem, putInfo)
}

class GatherPutFSM(val client: ActorRef, t: Int, stores: SelectionMemorize, putInfo: PutInfo)
  extends FSM[FsmState, FsmData] with ActorLogging {
  
  startWith(Collecting, Statuses(Nil))
  setTimer("send_by_timeout", OpsTimeout, t.seconds)
  
  when(Collecting) {
    case Event(GetResp(data), _) =>
      val vc: VectorClock = data match {
        case Some(d) if d.size == 1 => d.head.vc
        case Some(d) if d.size > 1 => (d map (_.vc)).foldLeft(new VectorClock)((sum, i) => sum.merge(i))
        case None => new VectorClock
      }
      val updatedData = Data(putInfo.key, putInfo.bucket, System.currentTimeMillis(), vc.:+(putInfo.localHashAdr.toString), putInfo.v)
      mapInPut(putInfo.nodes, updatedData, client)
      stay()
    
    case Event(incomeStatus: PutStatus, Statuses(statuses)) =>
      val updStatuses = Statuses( incomeStatus :: statuses )
      updStatuses.all.count(_ == Saved) match {
        case w if w == putInfo.W =>
          client ! AckSuccess
          goto(Sent) using ReceivedValues(putInfo.W)
        case _ => stay using updStatuses
      }

    case Event(OpsTimeout, _) =>
      client ! AckTimeoutFailed
      cancelTimer("send_by_timeout")
      stop()
  }
  
  when(Sent){
    case Event(status: PutStatus, ReceivedValues(n)) =>
      if(n + 1 == putInfo.N)
        stop()
      else
        stay using ReceivedValues(n + 1)
    case Event(OpsTimeout, _ ) =>
      cancelTimer("send_by_timeout")
      stop()
  }

  def mapInPut(nodes: List[Node], d: Data, client: ActorRef) = {
    val storeList = nodes.map(stores.get(_, "ring_write_store"))
      storeList.foreach(ref =>
      ref.fold(_.tell(StorePut(d), self),
        _.tell(StorePut(d), self)))
  }
  
  initialize()
}
