package mws.rng

import akka.actor.{ActorLogging, Props, ActorRef, FSM}
import akka.cluster.{Cluster}
import mws.rng.msg.{GetBucketResp, BucketPut, GetBucketVc, BucketVc, GetBucketIfNew, NoChanges, ReplFailed}
import mws.rng.data.{Data}
import scala.collection.SortedMap
import scala.concurrent.duration._

object ReplicationSupervisor {
  def props(buckets: SortedMap[Bucket, PreferenceList]): Props = Props(new ReplicationSupervisor(buckets))
}

/** Sequentially update buckets */
class ReplicationSupervisor(buckets: SortedMap[Bucket, PreferenceList]) extends FSM[FsmState, SortedMap[Bucket, PreferenceList]] with ActorLogging {
  val actorMem = SelectionMemorize(context.system)

  val local: Node = Cluster(context.system).selfAddress

  startWith(ReadyCollect, buckets)

  when(ReadyCollect){
    case Event("go-repl", data) =>
      data.headOption match {
        case None => 
          log.info("replication: skipped")
          stop()
        case Some((b, prefList)) => 
          log.info("replication: started")
          getBucketVc(b)
          goto (Sent) using data
      }
  }

  // after ask for vc of bucket
  when(Sent){
    case Event(b: BucketVc, data) =>
      val replica = data.head // safe
      val worker = context.actorOf(ReplicationWorker.props(prefList=replica._2))
      replica._2.map(node => actorMem.get(node, "ring_readonly_store").fold(
        _.tell(GetBucketIfNew(b=replica._1, b.vc), worker),
        _.tell(GetBucketIfNew(b=replica._1, b.vc), worker),
      ))
      goto (Collecting) using data
  }

  when(Collecting){
    case Event(b: Bucket, data) =>
      data - b match {
        case empty if empty.isEmpty =>
          log.info("replication: finished")
          stop()
        case remaining =>
          val replica = remaining.head // safe
          getBucketVc(replica._1)
          goto (Sent) using remaining
      }
    case Event(ReplFailed(down), _) =>
      //todo: check that memberRemoved is received after node is down
      log.info("replication: skipped with timeout")
      stop()
      // log.info("replication: start over with new pref list")
      // val data = buckets.map{ 
      //   case (b, prefList) => b -> (prefList -- down.map(AddressFromURIString(_)))
      // }.collect{
      //   case x@(b, prefList) if prefList.nonEmpty => x
      // }
      // data.headOption match {
      //   case None => 
      //     log.info("replication: skipped")
      //     stop()
      //   case Some((b, prefList)) => 
      //     log.info("replication: started")
      //     getBucketVc(b)
      //     goto (Sent) using data
      // }
  }

  def getBucketVc(b: Bucket): Unit = {
    actorMem.get(local, "ring_readonly_store").fold(
      _ ! GetBucketVc(b),
      _ ! GetBucketVc(b),
    )
  }
}

import ReplicationWorker.{ReplKeys}

object ReplicationWorker {
  final case class ReplKeys(prefList: PreferenceList, info: Seq[Seq[Data]])

  def props(prefList: PreferenceList): Props = Props(new ReplicationWorker(prefList))
}

class ReplicationWorker(_prefList: PreferenceList) extends FSM[FsmState, ReplKeys] with ActorLogging {
  import context.system
  val cluster = Cluster(system)
  val local = cluster.selfAddress
  val actorMem = SelectionMemorize(system)

  setTimer("send_by_timeout", OpsTimeout, 60 seconds)
  startWith(Collecting, ReplKeys(_prefList, info=Nil))

  when(Collecting){
    case Event(GetBucketResp(b, l), data) =>
      data.prefList - addr(sender) match {
        case empty if empty.isEmpty =>
          val all = data.info.foldLeft(l)((acc, list) => list ++ acc)
          val merged = mergeBucketData(all, Nil)
          actorMem.get(local, "ring_write_store").fold(
            _ ! BucketPut(merged),
            _ ! BucketPut(merged),
          )
          context.parent ! b
          stop()
        case nodes => stay() using ReplKeys(nodes, l +: data.info)
      }

    case Event(NoChanges(b), data) =>
      data.prefList - addr(sender) match {
        case empty if empty.isEmpty =>
          val all = data.info.foldLeft(Nil: Seq[Data])((acc, list) => list ++ acc)
          val merged = mergeBucketData(all, Nil)
          actorMem.get(local, "ring_write_store").fold(
            _ ! BucketPut(merged),
            _ ! BucketPut(merged),
          )
          context.parent ! b
          stop()
        case nodes => stay() using ReplKeys(nodes, data.info)
      }

    case Event(OpsTimeout, data) =>
      log.warning(s"replication: timeout. downing=${data.prefList}")
      data.prefList.map(cluster.down)
      context.parent ! ReplFailed(data.prefList.map(_.toString).toSeq)
      stop()
  }

  def addr(s: ActorRef): Node = s.path.address

  initialize()
}
