package zd.kvs
package rng

import zd.kvs.rng.data.{Data}
import zd.kvs.rng.GatherGet.AddrOfData
import scala.annotation.tailrec
import scala.collection.immutable.{HashMap, HashSet}
import zero.ext._, option._
import zd.proto.Bytes
import zd.kvs.rng.model.KeyBucketData

object MergeOps {
  def forDump(xs: Vector[KeyBucketData]): Vector[KeyBucketData] = {
    @tailrec def loop(xs: Vector[KeyBucketData], acc: Bytes HashMap KeyBucketData): Vector[KeyBucketData] = {
      xs match {
        case xs if xs.isEmpty => acc.values.toVector
        case received +: t =>
          val k = received.key
          acc.get(k) match {
            case None =>
              loop(t, acc + (k -> received))
            case Some(stored) =>
              (stored.data < received.data) match {
                case OkLess(true) => loop(t, acc + (k -> received))
                case OkLess(false) => loop(t, acc)
                case ConflictLess(true, vc) => loop(t, acc + (k -> received.copy(data=received.data.copy(vc=vc))))
                case ConflictLess(false, vc) => loop(t, acc + (k -> stored.copy(data=stored.data.copy(vc=vc))))
              }
          }
      }
    }
    loop(xs, acc=HashMap.empty)
  }

  def forRepl(xs: Vector[KeyBucketData]): Vector[KeyBucketData] = {
    @tailrec def loop(xs: Vector[KeyBucketData], acc: Bytes HashMap KeyBucketData): Vector[KeyBucketData] = {
      xs match {
        case xs if xs.isEmpty => acc.values.toVector
        case received +: t =>
          val k = received.key
          acc.get(k) match {
            case None =>
              loop(t, acc + (k -> received))
            case Some(stored) =>
              (stored.data < received.data) match {
                case OkLess(true) => loop(t, acc + (k -> received))
                case OkLess(false) => loop(t, acc)
                case ConflictLess(true, vc) => loop(t, acc + (k -> received.copy(data=received.data.copy(vc=vc))))
                case ConflictLess(false, vc) => loop(t, acc + (k -> stored.copy(data=stored.data.copy(vc=vc))))
              }
          }
      }
    }
    loop(xs, acc=HashMap.empty)
  }

  /* returns (actual data, list of outdated nodes) */
  def forGatherGet(xs: Vector[AddrOfData]): (Option[Data], HashSet[Node]) = {
    @tailrec
    def loop(xs: Vector[Option[Data]], newest: Option[Data]): Option[Data] = {
      xs match {
        case xs if xs.isEmpty => newest
        case None +: t => loop(t, newest)
        case (r@Some(received)) +: t =>
          newest match {
            case None => loop(t, r)
            case s@Some(saved) =>
              (saved < received) match {
                case OkLess(true) => loop(t, r)
                case OkLess(false) => loop(t, s)
                case ConflictLess(true, _) => loop(t, r)
                case ConflictLess(false, _) => loop(t, s)
              }
          }
      }
    }
    xs match {
      case Seq() => None -> HashSet.empty
      case h +: t =>
        val correct = loop(t.map(_._1), h._1)
        def makevc1(x: Option[Data]): VectorClock = x.map(_.vc).getOrElse(emptyVC)
        val correct_vc = makevc1(correct)
        correct -> xs.view.filterNot(x => makevc1(x._1) == correct_vc).map(_._2).to(HashSet)
    }
  }

  def forPut(stored: Option[Data], received: Data): Option[Data] = {
    stored match {
      case None => 
        received.some
      case Some(stored) =>
        (stored < received) match {
          case OkLess(true) => received.some
          case OkLess(false) => None
          case ConflictLess(true, vc) => received.copy(vc=vc).some
          case ConflictLess(false, vc) => stored.copy(vc=vc).some
        }
    }
  }

  sealed trait LessComp
  final case class OkLess(res: Boolean) extends LessComp
  final case class ConflictLess(res: Boolean, vc: VectorClock) extends LessComp

  implicit class DataExt(x: Data) {
    def <(o: Data): LessComp = {
      val xvc = x.vc
      val ovc = o.vc
      if (xvc < ovc) OkLess(true)
      else if (xvc == ovc) OkLess(x.lastModified < o.lastModified)
      else if (xvc > ovc) OkLess(false)
      else { // xvc <> ovc
        val mergedvc = xvc merge ovc
        if (x.lastModified < o.lastModified) ConflictLess(true, mergedvc)
        else ConflictLess(false, mergedvc)
      }
    }
  }
}
