package mws.rng

import akka.cluster.VectorClock
import mws.rng.data.{Data}
import mws.rng.GatherGet.AddrOfData
import scala.annotation.tailrec
import scala.collection.immutable.{HashMap}
import scalaz.Scalaz._

object MergeOps {
  def forDump(l: Seq[Data]): Seq[Data] = {
    @tailrec
    def loop(l: Seq[Data], merged: Key Map Data): Seq[Data] = l match {
      case h +: t =>
        val hvc = makevc(h.vc)
        merged.get(h.key) match {
          case Some(d) if hvc == makevc(d.vc) && h.lastModified > d.lastModified =>
            loop(t, merged + (h.key -> h))
          case Some(d) if hvc > makevc(d.vc) =>
            loop(t, merged + (h.key -> h))
          case Some(_) => loop(t, merged)
          case None => loop(t, merged + (h.key -> h))
        }
      case xs if xs.isEmpty => merged.values.toVector
    }
    loop(l, merged=HashMap.empty)
  }

  def forRepl(xs: Seq[Data]): Seq[Data] = {
    @tailrec
    def loop(xs: Seq[Data], acc: Key HashMap Data): Seq[Data] = {
      xs match {
        case xs if xs.isEmpty => acc.values.toSeq
        case h +: t =>
          acc.get(h.key) match {
            case None =>
              loop(t, acc + (h.key -> h))
            case Some(s) => // stored
              val hvc = makevc(h.vc)
              val svc = makevc(s.vc)
              if (hvc < svc) {
                loop(t, acc)
              } else if (hvc == svc) {
                if (s.lastModified <= h.lastModified) {
                  loop(t, acc + (h.key -> h))
                } else { // s.lastModified > h.lastModified
                  loop(t, acc)
                }
              } else if (hvc > svc) {
                loop(t, acc + (h.key -> h))
              } else { // hvc <> svc
                if (s.lastModified <= h.lastModified) {
                  loop(t, acc + (h.key -> h))
                } else { // s.lastModified > h.lastModified
                  loop(t, acc)
                }
              }
          }
      }
    }
    loop(xs, acc=HashMap.empty[Key,Data])
  }

  /* returns (actual data, list of outdated nodes) */
  def forGatherGet(xs: Vector[AddrOfData]): (Option[Data], Vector[Node]) = {
    @tailrec
    def findCorrect(xs: Vector[AddrOfData], newest: AddrOfData): AddrOfData = {
      xs match {
        case xs if xs.isEmpty => newest
        case h +: t if t.exists(age1(h) < age1(_)) => //todo: remove case (unit test)
          findCorrect(t, newest)
        case h +: t if age1(h) > age1(newest) =>
          findCorrect(t, h)
        case h +: t if age1(h) <> age1(newest) && age2(h) > age2(newest) => 
          findCorrect(t, h)
        case xs =>
          findCorrect(xs.tail, newest)
      }
    }
    def age1(d: AddrOfData): VectorClock = {
      d._1.fold(new VectorClock)(e => makevc(e.vc))
    }
    def age2(d: AddrOfData): Long = {
      d._1.fold(0L)(_.lastModified)
    }
    xs.map(age1(_)).toSet.size match {
      case 0 => None -> Vector.empty
      case 1 => xs.head._1 -> Vector.empty
      case n =>
        val correct = findCorrect(xs.tail, xs.head)
        correct._1 -> xs.filterNot(age1(_) == age1(correct)).map(_._2)
    }
  }

  def forPut(stored: Option[Data], received: Data): Option[Data] = {
    stored match {
      case None => 
        received.some
      case Some(s) =>
        val svc = makevc(s.vc)
        val rvc = makevc(received.vc)
        if (svc < rvc) {
          received.some
        } else if (svc == rvc) {
          if (s.lastModified <= received.lastModified) {
            received.some
          } else { // s.lastModified > received.lastModified
            None
          }
        } else if (svc > rvc) {
          None // ignore received
        } else { // svc <> rvc
          val mergedVc = fromvc(svc merge rvc)
          if (s.lastModified <= received.lastModified) {
            received.copy(vc=mergedVc).some
          } else { // s.lastModified > received.lastModified
            s.copy(vc=mergedVc).some // keep stored
          }
        }
    }
  }
}
