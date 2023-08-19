package kvs.rng

import scala.annotation.tailrec
import scala.collection.immutable.{HashMap, HashSet}

import data.Data, GatherGet.AddrOfData, model.KeyBucketData

private final class Bytes private (a: Array[Byte]) {
  lazy val length: Int = a.length
  lazy val isEmpty: Boolean = a.isEmpty
  lazy val nonEmpty: Boolean = a.nonEmpty
  lazy val mkString: String = new String(a, "utf8")
  val unsafeArray: Array[Byte] = a
  override def equals(other: Any): Boolean = {
    if (!other.isInstanceOf[Bytes]) false
    else java.util.Arrays.equals(a, other.asInstanceOf[Bytes].unsafeArray)
  }
  override def hashCode(): Int = java.util.Arrays.hashCode(a)
}

private object Bytes {
  val empty = new Bytes(Array.emptyByteArray)
  def unsafeWrap(a: Array[Byte]): Bytes = new Bytes(a)
  def apply(bs: Byte*): Bytes = new Bytes(bs.toArray)
}

object MergeOps {
  def forDump(xs: Vector[KeyBucketData]): Vector[KeyBucketData] = {
    @tailrec def loop(xs: Vector[KeyBucketData], acc: HashMap[Bytes, KeyBucketData]): Vector[KeyBucketData] = {
      xs match
        case Vector() => acc.values.toVector
        case Vector(received, s*) =>
          val t = s.toVector
          val k = Bytes.unsafeWrap(received.key)
          acc.get(k) match
            case None =>
              loop(t, acc + (k -> received))
            case Some(stored) =>
              (stored.data < received.data) match
                case OkLess(true) => loop(t, acc + (k -> received))
                case OkLess(false) => loop(t, acc)
                case ConflictLess(true, vc) => loop(t, acc + (k -> received.copy(data=received.data.copy(vc=vc))))
                case ConflictLess(false, vc) => loop(t, acc + (k -> stored.copy(data=stored.data.copy(vc=vc))))
    }
    loop(xs, acc=HashMap.empty)
  }

  def forRepl(xs: Vector[KeyBucketData]): Vector[KeyBucketData] =
    @tailrec
    def loop(xs: Vector[KeyBucketData], acc: HashMap[Bytes, KeyBucketData]): Vector[KeyBucketData] =
      xs match
        case Vector() => acc.values.toVector
        case Vector(received, s*) =>
          val t = s.toVector
          val k = Bytes.unsafeWrap(received.key)
          acc.get(k) match
            case None =>
              loop(t, acc + (k -> received))
            case Some(stored) =>
              (stored.data < received.data) match
                case OkLess(true) => loop(t, acc + (k -> received))
                case OkLess(false) => loop(t, acc)
                case ConflictLess(true, vc) => loop(t, acc + (k -> received.copy(data=received.data.copy(vc=vc))))
                case ConflictLess(false, vc) => loop(t, acc + (k -> stored.copy(data=stored.data.copy(vc=vc))))
    loop(xs, acc=HashMap.empty)

  /* returns (actual data, list of outdated nodes) */
  def forGatherGet(xs: Vector[AddrOfData]): (Option[Data], HashSet[Node]) =
    @tailrec
    def loop(xs: Vector[Option[Data]], newest: Option[Data]): Option[Data] =
      xs match
        case Vector() => newest
        case Vector(x, s*) =>
          x match
            case None => loop(s.toVector, newest)
            case r@Some(received) =>
              val t = s.toVector
              newest match
                case None => loop(t, r)
                case s@Some(saved) =>
                  saved < received match
                    case OkLess(true) => loop(t, r)
                    case OkLess(false) => loop(t, s)
                    case ConflictLess(true, _) => loop(t, r)
                    case ConflictLess(false, _) => loop(t, s)
    xs match
      case Vector() => None -> HashSet.empty
      case Vector(h, t*) =>
        val correct = loop(t.view.map(_._1).toVector, h._1)
        def makevc1(x: Option[Data]): VectorClock = x.map(_.vc).getOrElse(emptyVC)
        val correct_vc = makevc1(correct)
        correct -> xs.view.filterNot(x => makevc1(x._1) == correct_vc).map(_._2).to(HashSet)

  def forPut(stored: Option[Data], received: Data): Option[Data] =
    stored match
      case None => 
        Some(received)
      case Some(stored) =>
        (stored < received) match
          case OkLess(true) => Some(received)
          case OkLess(false) => None
          case ConflictLess(true, vc) => Some(received.copy(vc=vc))
          case ConflictLess(false, vc) => Some(stored.copy(vc=vc))

  sealed trait LessComp
  case class OkLess(res: Boolean) extends LessComp
  case class ConflictLess(res: Boolean, vc: VectorClock) extends LessComp

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
