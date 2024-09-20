package kvs
package sort

import com.google.protobuf.{CodedOutputStream, CodedInputStream}
import proto.*
import scala.math.Ordering
import scala.math.Ordering.Implicits.infixOrderingOps
import zd.rng.*
import zio.*, stream.*

trait Sort:
  def insert[A: Codec: Ordering](ns: String, x: A)(using CanEqual[None.type, Option[Node[A]]]): UIO[Unit]
  def remove[A: Codec: Ordering](ns: String, x: A)(using CanEqual[None.type, Option[Node[A]]]): UIO[Unit]
  def flatten[A: Codec](ns: String)(using CanEqual[None.type, Option[Node[A]]]): UStream[A]
end Sort

type Key = Long

case class Node[A](
  @N(1) left: Option[Key]
, @N(2) x: A
, @N(3) right: Option[Key]
, @N(4) active: Boolean
)

object Node:
  def apply[A](x: A): Node[A] = Node(left=None, x, right=None, active=true)
end Node

def insert[A: Codec: Ordering](ns: String, x: A)(using CanEqual[None.type, Option[Node[A]]]): URIO[Sort, Unit] =
  ZIO.serviceWithZIO(_.insert(ns, x))

def remove[A: Codec: Ordering](ns: String, x: A)(using CanEqual[None.type, Option[Node[A]]]): URIO[Sort, Unit] =
  ZIO.serviceWithZIO(_.remove(ns, x))

def flatten[A: Codec](ns: String)(using CanEqual[None.type, Option[Node[A]]]): ZStream[Sort, Nothing, A] =
  ZStream.serviceWithStream(_.flatten(ns))

class SortImpl(dba: Dba)(using CanEqual[None.type, Option[Value]]) extends Sort:
  def equiv[A: Ordering](x: A, y: A): Boolean = implicitly[Ordering[A]].equiv(x, y)

  def insert[A: Codec: Ordering](ns: String, x: A)(using CanEqual[None.type, Option[Node[A]]]): UIO[Unit] =
    for
      node <- dba_get(ns, dba_head)
      _ <-
        node match
          case None => dba_add(ns, Node(x))
          case Some(node) => insert(ns, x, node, dba_head)
    yield ()

  private def insert[A: Codec: Ordering](ns: String, x: A, node: Node[A], nodeKey: Key)(using CanEqual[None.type, Option[Node[A]]]): UIO[Unit] =
    node match
      case Node(_, y, _, false) if equiv(x, y) =>
        dba_put(ns, nodeKey, node.copy(active=true))

      case Node(_, y, _, true) if equiv(x, y) => ZIO.unit

      case Node(None, y, _, _) if x < y =>
        for
          k <- dba_add(ns, Node(x))
          _ <- dba_put(ns, nodeKey, node.copy(left=Some(k)))
        yield ()

      case Node(Some(t), y, _, _) if x < y =>
        for
          node1 <- dba_get(ns, t)
          _ <-
            node1 match
              case None =>
                for
                  k <- dba_add(ns, Node(x))
                  _ <- dba_put(ns, nodeKey, node.copy(left=Some(k)))
                yield ()
              case Some(node1) =>
                insert(ns, x, node1, t)
        yield ()

      case Node(_, _, None, _) =>
        for
          k <- dba_add(ns, Node(x))
          _ <- dba_put(ns, nodeKey, node.copy(right=Some(k)))
        yield ()

      case Node(_, _, Some(s), _) =>
        for
          node1 <- dba_get(ns, s)
          _ <-
            node1 match
              case None =>
                for
                  k <- dba_add(ns, Node(x))
                  _ <- dba_put(ns, nodeKey, node.copy(right=Some(k)))
                yield ()
              case Some(node1) =>
                insert(ns, x, node1, s)
        yield ()

  def remove[A: Codec: Ordering](ns: String, x: A)(using CanEqual[None.type, Option[Node[A]]]): UIO[Unit] =
    for
      node <- dba_get(ns, dba_head)
      _ <-
        node match
          case None => ZIO.unit
          case Some(node) => remove(ns, x, node, dba_head)
    yield ()

  private def remove[A: Codec: Ordering](ns: String, x: A, node: Node[A], nodeKey: Key)(using CanEqual[None.type, Option[Node[A]]]): UIO[Unit] =
    node match
      case Node(_, y, _, false) if equiv(x, y) => ZIO.unit

      case Node(_, y, _, true) if equiv(x, y) =>
        dba_put(ns, nodeKey, node.copy(active=false))

      case Node(None, y, _, _) if x < y => ZIO.unit

      case Node(Some(t), y, _, _) if x < y =>
        for
          node1 <- dba_get(ns, t)
          _ <-
            node1 match
              case None => ZIO.unit
              case Some(node1) =>
                remove(ns, x, node1, t)
        yield ()

      case Node(_, _, None, _) => ZIO.unit

      case Node(_, _, Some(s), _) =>
        for
          node1 <- dba_get(ns, s)
          _ <-
            node1 match
              case None => ZIO.unit
              case Some(node1) =>
                remove(ns, x, node1, s)
        yield ()

  def flatten[A: Codec](ns: String)(using CanEqual[None.type, Option[Node[A]]]): UStream[A] =
    flatten(ns, dba_head)

  private def flatten[A: Codec](ns: String, nodeKey: Key)(using CanEqual[None.type, Option[Node[A]]]): UStream[A] =
    for
      node <- ZStream.fromZIO(dba_get(ns, nodeKey))
      xs <- node match
        case None => ZStream.empty
        case Some(Node(t, x, s, a)) =>
          val xs = ZStream(x).filter(_ => a)
          val ts = ZStream(t).collectSome.flatMap(flatten(ns, _))
          val ss = ZStream(s).collectSome.flatMap(flatten(ns, _))
          ts ++ xs ++ ss
    yield xs

  def dba_add[A: Codec](ns: String, v: Node[A]): UIO[Key] =
    for
      nse <- encodeNS(ns)
      ide <- dba.get(nse)
      id <- ide match
        case Some(ide) => decodeKeyAsValue(ide)
        case None => ZIO.succeed(0L)
      id1 <- ZIO.succeed(id + 1L)
      id1e <- encodeKeyAsValue(id1)
      _ <- dba.put(nse, id1e)
      _ <- dba_put(ns, id1, v)
    yield id1
  
  def dba_put[A: Codec](ns: String, k: Key, v: Node[A]): UIO[Unit] =
    for
      ke <- encodeKey(ns, k)
      ve <- ZIO.succeed(encode(v))
      _ <- dba.put(ke, ve)
    yield ()
  
  def dba_get[A: Codec](ns: String, k: Key): UIO[Option[Node[A]]] =
    for
      ke <- encodeKey(ns, k)
      ve <- dba.get(ke)
      v <- ve match
        case None => ZIO.none
        case Some(ve) => ZIO.succeed(decode[Node[A]](ve)).asSome
    yield v

  val dba_head: Key = 1L

  def decodeKeyAsValue(bs: Array[Byte]): UIO[Key] =
    ZIO
      .attempt:
        val cis = CodedInputStream.newInstance(bs).nn
        cis.readUInt64
      .orDie
  
  def encodeKeyAsValue(k: Key): UIO[Array[Byte]] =
    ZIO
      .attempt:
        if k <= 0 then throw RuntimeException("key is not positive")
        val size = CodedOutputStream.computeUInt64SizeNoTag(k)
        val bs = new Array[Byte](size)
        val cos = CodedOutputStream.newInstance(bs).nn
        cos.writeUInt64NoTag(k)
        bs
      .orDie
  
  def encodeKey(ns: String, k: Key): UIO[Array[Byte]] =
    ZIO
      .attempt:
        if k <= 0 then throw RuntimeException("key is not positive")
        val size = CodedOutputStream.computeStringSizeNoTag(ns) + 1 + CodedOutputStream.computeUInt64SizeNoTag(k)
        val bs = new Array[Byte](size)
        val cos = CodedOutputStream.newInstance(bs).nn
        cos.writeStringNoTag(ns)
        cos.write(0x9: Byte)
        cos.writeUInt64NoTag(k)
        bs
      .orDie

  def encodeNS(ns: String): UIO[Array[Byte]] =
    ZIO
      .attempt:
        val size = CodedOutputStream.computeStringSizeNoTag(ns)
        val bs = new Array[Byte](size)
        val cos = CodedOutputStream.newInstance(bs).nn
        cos.writeStringNoTag(ns)
        bs
      .orDie

  given [A: Codec]: Codec[Node[A]] = caseCodecAuto
end SortImpl

object SortImpl:
  val layer: URLayer[Dba, Sort] =
    given CanEqual[None.type, Option[Value]] = CanEqual.derived
    ZLayer.fromFunction(SortImpl(_))
end SortImpl
