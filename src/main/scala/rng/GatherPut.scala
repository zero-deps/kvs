package zd.rng

import akka.actor.{ActorLogging, ActorRef, FSM, Props, RootActorPath}
import zd.rng.data.Data
import zd.rng.model.{StoreGetAck, StorePut}
import scala.concurrent.duration._
import scalaz.Scalaz._
import java.util.Arrays

final class PutInfo(
    val key: Key
  , val v: Value
  , val N: Int
  , val W: Int
  , val bucket: Bucket
  , val localAdr: Node
  , val nodes: Set[Node]
  ) {
  override def equals(other: Any): Boolean = other match {
    case that: PutInfo =>
      Arrays.equals(key, that.key) &&
      Arrays.equals(v, that.v) &&
      N == that.N &&
      W == that.W &&
      bucket == that.bucket &&
      localAdr == that.localAdr &&
      nodes == that.nodes
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq(key, v, N, W, bucket, localAdr, nodes)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  override def toString = s"PutInfo(key=$key, v=$v, N=$N, W=$W, bucket=$bucket, localAdr=$localAdr, nodes=$nodes)"
}

object PutInfo {
  def apply(key: Key, v: Value, N: Int, W: Int, bucket: Bucket, localAdr: Node, nodes: Set[Node]): PutInfo = {
    new PutInfo(key=key, v=v, N=N, W=W, bucket=bucket, localAdr=localAdr, nodes=nodes)
  }
}

object GatherPut {
  def props(client: ActorRef, t: FiniteDuration, actorsMem: SelectionMemorize, putInfo: PutInfo): Props = Props(new GatherPut(client, t, actorsMem, putInfo))
}

class GatherPut(client: ActorRef, t: FiniteDuration, stores: SelectionMemorize, putInfo: PutInfo) extends FSM[FsmState, Int] with ActorLogging {

  startWith(Collecting, 0)
  setTimer("send_by_timeout", "timeout", t)

  when(Collecting){
    case Event(StoreGetAck(data), _) =>
      val vc = if (data.size == 1) {
        data.head.vc
      } else if (data.size > 1) {
        data.map(_.vc).foldLeft(emptyVC)((sum, i) => sum.merge(i))
      } else {
        emptyVC
      }
      val updatedData = Data(putInfo.key, putInfo.bucket, now_ms(), vc.:+(putInfo.localAdr.toString), putInfo.v)
      mapInPut(putInfo.nodes, updatedData)
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
        stay using n1
      }

    case Event("timeout", _) =>
      client ! AckTimeoutFailed(s"put=${putInfo.key}")
      stop()
  }
  
  // keep fsm running to avoid dead letter warnings
  when(Sent){
    case Event("ok", n) =>
      val n1 = n + 1
      if (n1 == putInfo.N) stop()
      else stay using n1
    case Event("timeout", _) =>
      stop()
  }

  def mapInPut(nodes: Set[Node], d: Data) = {
    val storeList = nodes.map(n => RootActorPath(n) / "user" / "ring_write_store")
      storeList.foreach(ref => context.system.actorSelection(ref).tell(StorePut(d), self))
  }
  
  initialize()
}
