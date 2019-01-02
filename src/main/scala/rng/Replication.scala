package mws.rng

import akka.actor.{ActorLogging, Props, ActorRef, FSM}
import akka.cluster.{Cluster, VectorClock}
import mws.rng.data.{Data}
import mws.rng.msg_repl.{ReplBucketPut, ReplGetBucketsVc, ReplBucketsVc, ReplGetBucketIfNew, ReplBucketUpToDate, ReplNewerBucketData, ReplBucketDataItem, ReplVectorClock}
import scala.annotation.tailrec
import scala.collection.immutable.{SortedMap, HashMap}
import scala.concurrent.duration.{Duration}
import scalaz.Scalaz._

import ReplicationSupervisor.{State}

object ReplicationSupervisor {
  final case class Progress(done: Int, total: Int, step: Int)
  final case class State(buckets: SortedMap[Bucket, PreferenceList], bvcs: Map[Bucket, ReplVectorClock], progress: Progress)

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
        val bs: Seq[Bucket] = state.buckets.keys.toSeq
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

  def getBucketIfNew(b: Bucket, prefList: PreferenceList, _bvc: Option[ReplVectorClock]): Unit = {
    val bvc = _bvc.map(_.vs).getOrElse(Nil)
    val worker = context.actorOf(ReplicationWorker.props(b, prefList, bvc))
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
          if (pr.done % pr.step === 0) log.info(s"${pr.done*100/pr.total}%")
          val (b, prefList) = remaining.head // safe
          val bvc = state.bvcs.get(b)
          getBucketIfNew(b, prefList, bvc)
          stay using state.copy(buckets=remaining, progress=pr.copy(done=pr.done+1))
      }
  }
}

import ReplicationWorker.{ReplState}

object ReplicationWorker {
  final case class ReplState(prefList: PreferenceList, info: Seq[Seq[Data]], vc: VectorClock)

  def props(b: Bucket, prefList: PreferenceList, vc: VectorClockList): Props = Props(new ReplicationWorker(b, prefList, vc))

  def mergeBucketData(l: Seq[Data]): Seq[ReplBucketDataItem] = mergeBucketData(l, acc=HashMap.empty[Key,Seq[Data]])

  @tailrec
  private def mergeBucketData(l: Seq[Data], acc: Key Map Seq[Data]): Seq[ReplBucketDataItem] = l match {
    case h +: t =>
      acc.get(h.key) match {
        case Some(x +: Seq()) if makevc(h.vc) == makevc(x.vc) =>
          if (h.lastModified > x.lastModified) mergeBucketData(t, acc + (h.key -> Seq(h)))
          else mergeBucketData(t, acc)
        case Some(x +: Seq()) if makevc(h.vc) > makevc(x.vc) =>
          mergeBucketData(t, acc + (h.key -> Seq(h)))
        case Some(x +: Seq()) if makevc(h.vc) < makevc(x.vc) =>
          mergeBucketData(t, acc)
        case Some(xs) =>
          mergeBucketData(t, acc + (h.key -> (xs :+ h)))
        case None =>
          mergeBucketData(t, acc + (h.key -> Seq(h)))
      }
    case xs if xs.isEmpty => acc.map{ case (k,v) => ReplBucketDataItem(k,v) }.toSeq
  }
}

class ReplicationWorker(b: Bucket, _prefList: PreferenceList, _vc: VectorClockList) extends FSM[FsmState, ReplState] with ActorLogging {
  import ReplicationWorker.mergeBucketData
  import context.system
  val cluster = Cluster(system)
  val local = cluster.selfAddress
  val actorMem = SelectionMemorize(system)

  setTimer("send_by_timeout", "timeout", Duration.fromNanos(context.system.settings.config.getDuration("ring.gather-timeout-replication").toNanos), repeat=true)
  startWith(Collecting, ReplState(_prefList, info=Nil, makevc(_vc)))

  when(Collecting){
    case Event("start", state) =>
      // ask only remaining nodes (state.prefList) with original VC (_vc)
      state.prefList.map(node => actorMem.get(node, "ring_readonly_store").fold(
        _ ! ReplGetBucketIfNew(b, _vc),
        _ ! ReplGetBucketIfNew(b, _vc),
      ))
      stay using state

    case Event(ReplNewerBucketData(vc, items), state) =>
      val l: Seq[Data] = items.flatMap(_.data) //todo: replace `l` with `items`
      if (state.prefList contains addr(sender)) {
        state.prefList - addr(sender) match {
          case empty if empty.isEmpty =>
            val all = state.info.foldLeft(l)((acc, list) => list ++ acc)
            val merged = mergeBucketData(all)
            actorMem.get(local, "ring_write_store").fold(
              _ ! ReplBucketPut(b, fromvc(state.vc merge makevc(vc)), merged),
              _ ! ReplBucketPut(b, fromvc(state.vc merge makevc(vc)), merged),
            )
            context.parent ! b
            stop()
          case nodes =>
            stay using state.copy(
              prefList = nodes,
              info = l +: state.info, 
              vc = state.vc merge makevc(vc),
            )
        }
      } else {
        // after restart it is possible to receive multiple answers from same node
        stay using state
      }

    case Event(ReplBucketUpToDate(), state) =>
      self forward ReplNewerBucketData(vc=Nil, items=Nil)
      stay using state

    case Event("timeout", state) =>
      log.info(s"no answer. repeat with=${state.prefList}")
      self ! "start"
      stay using state
  }

  def addr(s: ActorRef): Node = s.path.address

  initialize()
}
