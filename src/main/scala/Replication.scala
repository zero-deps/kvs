package mws.rng

import akka.actor.{ActorLogging, Props, ActorRef, FSM}
import akka.cluster.Cluster
import mws.rng.store.{GetBucketResp, BucketPut, BucketGet}
import scala.collection.SortedMap
import scala.concurrent.duration._

/** Sequentially update buckets.*/
class ReplicationSupervisor(buckets: SortedMap[Bucket, PreferenceList]) extends FSM[FsmState, SortedMap[Bucket, PreferenceList]]
  with ActorLogging{
  val actorMem = SelectionMemorize(context.system)
  startWith(ReadyCollect, buckets)

  when(ReadyCollect){
    case Event("go-repl", data) =>
      log.info(s"Replication is started")
      val replica = data.head
      replica._2.map(node => actorMem.get(node, "ring_readonly_store").fold(
            _ ! BucketGet(replica._1), _ ! BucketGet(replica._1)))  
      context.system.actorOf(Props(classOf[ReplicationWorker], replica._1, replica._2))
      goto(Collecting) using data
  }

  when(Collecting){
    case Event(b: Bucket, data) =>
      data - b match {
        case empty if empty.isEmpty =>
          log.info(s"Replication is finished")
          stop()
        case syncBuckets =>
          val replica = syncBuckets.head
          replica._2.map(node => actorMem.get(node, "ring_readonly_store").fold(
            _ ! BucketGet(replica._1), _ ! BucketGet(replica._1)))  
          context.system.actorOf(Props(classOf[ReplicationWorker], replica._1, replica._2))
          stay() using syncBuckets
      }
  }
}

case class ReplKeys(b:Bucket, prefList: PreferenceList, info: List[List[Data]])
class ReplicationWorker(bucket:Bucket,preferenceList: PreferenceList) extends FSM[FsmState, ReplKeys] with ActorLogging {
  val local = Cluster(context.system).selfAddress
  val actorMem = SelectionMemorize(context.system)

  setTimer("send_by_timeout", OpsTimeout, context.system.settings.config.getInt("ring.gather-timeout").seconds)
  startWith(Collecting, ReplKeys(bucket, preferenceList, Nil))

  when(Collecting){
    case Event(GetBucketResp(b,l), data) =>
      data.prefList - addrs(sender()) match {
        case empty if empty.isEmpty =>
          val all = data.info.foldLeft(l)((acc, list) => list :::  acc )
          val merged = mergeBucketData(all, Nil)
          actorMem.get(local, "ring_write_store").fold(_ ! BucketPut(merged), _ ! BucketPut(merged))
          context.parent ! bucket
          stop()
        case nodes => stay() using ReplKeys(bucket, nodes, l :: data.info)
      }

    case Event(OpsTimeout, data) =>
      self ! GetBucketResp(bucket, Nil)
      stay()
  }

  def addrs(s: ActorRef) = if(s.path.address.hasLocalScope) local else s.path.address

  initialize()
}
