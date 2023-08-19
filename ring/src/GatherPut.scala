package kvs.rng

import org.apache.pekko.actor.{ActorLogging, ActorRef, FSM, Props, RootActorPath}
import scala.concurrent.duration.*

import data.Data, model.{StoreGetAck, StorePut}

case class PutInfo(
    key: Array[Byte]
  , v: Array[Byte]
  , N: Int
  , W: Int
  , bucket: Bucket
  , localAdr: Node
  , nodes: Set[Node]
  )

object GatherPut {
  def props(client: ActorRef, t: FiniteDuration, putInfo: PutInfo): Props = Props(new GatherPut(client, t, putInfo))
}

class GatherPut(client: ActorRef, t: FiniteDuration, putInfo: PutInfo) extends FSM[FsmState, Int] with ActorLogging {

  startWith(Collecting, 0)
  setTimer("send_by_timeout", "timeout", t)

  when(Collecting){
    case Event(StoreGetAck(key, bucket, data), _) =>
      val vc = if (data.size == 1) {
        data.head.vc
      } else if (data.size > 1) {
        data.map(_.vc).foldLeft(emptyVC)((sum, i) => sum.merge(i))
      } else {
        emptyVC
      }
      val updatedData = Data(now_ms(), vc.:+(putInfo.localAdr.toString), putInfo.v)
      mapInPut(putInfo.nodes, key=key, bucket=bucket, updatedData)
      stay()
    
    case Event("ok", n) =>
      val n1 = n + 1
      if (n1 == putInfo.N) {
        client ! AckSuccess(None)
        stop()
      } else if (n1 == putInfo.W) {
        client ! AckSuccess(None)
        goto (Sent) using n1
      } else {
        stay() using n1
      }

    case Event("timeout", _) =>
      client ! AckTimeoutFailed("put", putInfo.key)
      stop()
  }
  
  // keep fsm running to avoid dead letter warnings
  when(Sent){
    case Event("ok", n) =>
      val n1 = n + 1
      if (n1 == putInfo.N) stop()
      else stay() using n1
    case Event("timeout", _) =>
      stop()
  }

  def mapInPut(nodes: Set[Node], key: Array[Byte], bucket: Int, d: Data) = {
    val storeList = nodes.map(n => RootActorPath(n) / "user" / "ring_write_store")
      storeList.foreach(ref => context.system.actorSelection(ref).tell(StorePut(key=key, bucket=bucket, d), self))
  }
  
  initialize()
}
