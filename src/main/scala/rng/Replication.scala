package zd.kvs
package rng

import akka.actor.{ActorLogging, Props, FSM}
import akka.cluster.{Cluster}
import scala.collection.immutable.{SortedMap}
import scala.concurrent.duration.{Duration}
import zd.kvs.rng.model.{ReplBucketPut, ReplBucketsVc, ReplGetBucketIfNew, ReplBucketUpToDate, ReplNewerBucketData, KeyBucketData}
import zd.kvs.rng.ReplicationSupervisor.{State}
import zd.kvs.rng.ReplicationSupervisor.ReplGetBucketsVc

object ReplicationSupervisor {
  final case class Progress(done: Int, total: Int, step: Int)
  final case class State(buckets: SortedMap[Bucket, PreferenceList], bvcs: Map[Bucket, VectorClock], progress: Progress)
  final case class ReplGetBucketsVc(bs: Vector[Int])

  def props(buckets: SortedMap[Bucket, PreferenceList]): Props = {
    val len = buckets.size
    Props(new ReplicationSupervisor(State(buckets, bvcs=Map.empty, Progress(done=0, total=len, step=len/4))))
  }
}

class ReplicationSupervisor(initialState: State) extends FSM[FsmState, State] with ActorLogging {
  val actorMem = SelectionMemorize(context.system)
  val local: Node = Cluster(context.system).selfAddress

  startWith(ReadyCollect, initialState)

  when(ReadyCollect){
    case Event("go-repl", state) =>
      log.info("started".green)
      if (state.buckets.isEmpty) {
        log.info("nothing to sync".green)
        stop()
      } else {
        val bs: Vector[Bucket] = state.buckets.keys.toVector
        log.info("asking for vector clocks for buckets")
        actorMem.get(local, "ring_readonly_store").fold(
          _ ! ReplGetBucketsVc(bs),
          _ ! ReplGetBucketsVc(bs),
        )
        goto (Sent) using state
      }
  }

  // after ask for vc of buckets
  when(Sent){
    case Event(ReplBucketsVc(bvcs), state) =>
      log.info("got vector clocks for buckets")
      state.buckets.headOption match {
        case None =>
          log.error(s"unexpected termination. state=${state}")
          stop()
        case Some((b, prefList)) =>
          val bvc = bvcs.get(b)
          getBucketIfNew(b, prefList, bvc)
          goto (Collecting) using state.copy(bvcs=bvcs)
      }
  }

  def getBucketIfNew(b: Bucket, prefList: PreferenceList, bvc: Option[VectorClock]): Unit = {
    val worker = context.actorOf(ReplicationWorker.props(b, prefList, bvc.getOrElse(emptyVC)))
    worker ! "start"
  }

  when(Collecting){
    case Event(b: Bucket, state) =>
      state.buckets - b match {
        case empty if empty.isEmpty =>
          log.info("finished".green)
          stop()
        case remaining =>
          val pr = state.progress
          if (pr.done % pr.step == 0) log.info(s"${pr.done*100/pr.total}%")
          val (b, prefList) = remaining.head // safe
          val bvc = state.bvcs.get(b)
          getBucketIfNew(b, prefList, bvc)
          stay using state.copy(buckets=remaining, progress=pr.copy(done=pr.done+1))
      }
  }
}

import ReplicationWorker.{ReplState}

object ReplicationWorker {
  final case class ReplState(prefList: PreferenceList, info: Vector[Vector[KeyBucketData]], vc: VectorClock)

  def props(b: Bucket, prefList: PreferenceList, vc: VectorClock): Props = Props(new ReplicationWorker(b, prefList, vc))
}

class ReplicationWorker(b: Bucket, _prefList: PreferenceList, _vc: VectorClock) extends FSM[FsmState, ReplState] with ActorLogging {
  import context.system
  val cluster = Cluster(system)
  val local = cluster.selfAddress
  val actorMem = SelectionMemorize(system)

  startTimerAtFixedRate("send_by_timeout", "timeout", Duration.fromNanos(context.system.settings.config.getDuration("ring.repl-timeout").toNanos))
  startWith(Collecting, ReplState(_prefList, info=Vector.empty, _vc))

  when(Collecting){
    case Event("start", state) =>
      // ask only remaining nodes (state.prefList) with original VC (_vc)
      state.prefList.map(node => actorMem.get(node, "ring_readonly_store").fold(
        _ ! ReplGetBucketIfNew(b, _vc),
        _ ! ReplGetBucketIfNew(b, _vc),
      ))
      stay using state

    case Event(ReplNewerBucketData(vc, items), state) =>
      if (state.prefList contains addr(sender)) {
        state.prefList - addr(sender) match {
          case empty if empty.isEmpty =>
            val all = state.info.foldLeft(items)((acc, list) => list ++ acc)
            val merged = MergeOps.forRepl(all)
            if (merged.nonEmpty) {
              actorMem.get(local, "ring_write_store").fold(
                _ ! ReplBucketPut(b, state.vc merge vc, merged),
                _ ! ReplBucketPut(b, state.vc merge vc, merged),
              )
            }
            context.parent ! b
            stop()
          case nodes =>
            stay using state.copy(
              prefList = nodes,
              info = items +: state.info, 
              vc = state.vc merge vc,
            )
        }
      } else {
        // after restart it is possible to receive multiple answers from same node
        stay using state
      }

    case Event(ReplBucketUpToDate, state) =>
      self forward ReplNewerBucketData(vc=emptyVC, items=Vector.empty)
      stay using state

    case Event("timeout", state) =>
      log.info(s"no answer. repeat with=${state.prefList}")
      self ! "start"
      stay using state
  }

  initialize()
}
