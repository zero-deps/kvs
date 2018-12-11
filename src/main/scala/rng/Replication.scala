package mws.rng

import akka.actor.{ActorLogging, Props, ActorRef, FSM}
import akka.cluster.{Cluster, VectorClock}
import mws.rng.data.{Data}
import mws.rng.msg.{BucketPut, GetBucketVc, BucketVc, GetBucketIfNew, ReplFailed, BucketUpToDate, NewerBucket}
import scala.collection.SortedMap
import scala.concurrent.duration.{Duration}

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
      val worker = {
        val vc = makevc(b.vc)
        context.actorOf(ReplicationWorker.props(prefList=replica._2, vc))
      }
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
      log.info("replication: skipped with timeout")
      stop()
  }

  def getBucketVc(b: Bucket): Unit = {
    actorMem.get(local, "ring_readonly_store").fold(
      _ ! GetBucketVc(b),
      _ ! GetBucketVc(b),
    )
  }
}

import ReplicationWorker.{ReplState}

object ReplicationWorker {
  final case class ReplState(prefList: PreferenceList, info: Seq[Seq[Data]], vc: VectorClock)

  def props(prefList: PreferenceList, vc: VectorClock): Props = Props(new ReplicationWorker(prefList, vc))
}

class ReplicationWorker(_prefList: PreferenceList, _vc: VectorClock) extends FSM[FsmState, ReplState] with ActorLogging {
  import context.system
  val cluster = Cluster(system)
  val local = cluster.selfAddress
  val actorMem = SelectionMemorize(system)

  setTimer("send_by_timeout", OpsTimeout, Duration.fromNanos(context.system.settings.config.getDuration("ring.gather-timeout-replication").toNanos), repeat=false)
  startWith(Collecting, ReplState(_prefList, info=Nil, _vc))

  when(Collecting){
    case Event(NewerBucket(b, vc, l), data) =>
      data.prefList - addr(sender) match {
        case empty if empty.isEmpty =>
          val all = data.info.foldLeft(l)((acc, list) => list ++ acc)
          val merged = mergeBucketData(all, Nil)
          actorMem.get(local, "ring_write_store").fold(
            _ ! BucketPut(merged, fromvc(data.vc)),
            _ ! BucketPut(merged, fromvc(data.vc)),
          )
          context.parent ! b
          stop()
        case nodes =>
          stay() using data.copy(
            prefList = nodes, 
            info = l +: data.info, 
            vc = data.vc merge makevc(vc),
          )
      }

    case Event(BucketUpToDate(b), data) =>
      data.prefList - addr(sender) match {
        case empty if empty.isEmpty =>
          val all = data.info.foldLeft(Nil: Seq[Data])((acc, list) => list ++ acc)
          val merged = mergeBucketData(all, Nil)
          actorMem.get(local, "ring_write_store").fold(
            _ ! BucketPut(merged, fromvc(data.vc)),
            _ ! BucketPut(merged, fromvc(data.vc)),
          )
          context.parent ! b
          stop()
        case nodes => stay() using data.copy(prefList=nodes)
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
