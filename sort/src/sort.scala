package kvs.sort

import com.google.protobuf.{CodedOutputStream, CodedInputStream}
import java.io.IOException
import kvs.rng.{Dba, DbaErr}
import proto.*
import scala.math.Ordering
import scala.math.Ordering.Implicits.infixOrderingOps
import zio.*, stream.*

trait Sort:
  def insert[A: Codec: Ordering](ns: String, x: A)(using CanEqual[A, A]): IO[Err, Unit]
  def remove[A: Codec: Ordering](ns: String, x: A)(using CanEqual[A, A]): IO[Err, Unit]
  def flatten[A: Codec](ns: String): Stream[Err, A]
end Sort

type Err = DbaErr | IOException

def insert[A: Codec: Ordering](ns: String, x: A)(using CanEqual[A, A]): ZIO[Sort, Err, Unit] =
  ZIO.serviceWithZIO(_.insert(ns, x))

def remove[A: Codec: Ordering](ns: String, x: A)(using CanEqual[A, A]): ZIO[Sort, Err, Unit] =
  ZIO.serviceWithZIO(_.remove(ns, x))

def flatten[A: Codec](ns: String): ZStream[Sort, Err, A] =
  ZStream.serviceWithStream(_.flatten(ns))

class SortLive(dba: Dba) extends Sort:
  def insert[A: Codec: Ordering](ns: String, x: A)(using CanEqual[A, A]): IO[Err, Unit] =
    for
      h <- dba_head
      node <- dba_get(ns, h)
      _ <-
        node match
          case None => dba_add(ns, toNode(x))
          case Some(node) => insert(ns, x, node, h)
    yield ()

  private def insert[A: Codec: Ordering](ns: String, x: A, node: Node[A], nodeKey: Key)(using CanEqual[A, A]): IO[Err, Unit] =
    node match
      case Node(_, y, _, false) if x == y =>
        dba_put(ns, nodeKey, node.copy(active=true))

      case Node(_, y, _, true) if x == y => ZIO.unit

      case Node(None, y, _, _) if x < y =>
        for
          k <- dba_add(ns, toNode(x))
          _ <- dba_put(ns, nodeKey, node.copy(left=Some(k)))
        yield ()

      case Node(Some(t), y, _, _) if x < y =>
        for
          node1 <- dba_get(ns, t)
          _ <-
            node1 match
              case None =>
                for
                  k <- dba_add(ns, toNode(x))
                  _ <- dba_put(ns, nodeKey, node.copy(left=Some(k)))
                yield ()
              case Some(node1) =>
                insert(ns, x, node1, t)
        yield ()

      case Node(_, _, None, _) =>
        for
          k <- dba_add(ns, toNode(x))
          _ <- dba_put(ns, nodeKey, node.copy(right=Some(k)))
        yield ()

      case Node(_, _, Some(s), _) =>
        for
          node1 <- dba_get(ns, s)
          _ <-
            node1 match
              case None =>
                for
                  k <- dba_add(ns, toNode(x))
                  _ <- dba_put(ns, nodeKey, node.copy(right=Some(k)))
                yield ()
              case Some(node1) =>
                insert(ns, x, node1, s)
        yield ()

  def remove[A: Codec: Ordering](ns: String, x: A)(using CanEqual[A, A]): IO[Err, Unit] =
    for
      h <- dba_head
      node <- dba_get(ns, h)
      _ <-
        node match
          case None => ZIO.unit
          case Some(node) => remove(ns, x, node, h)
    yield ()

  private def remove[A: Codec: Ordering](ns: String, x: A, node: Node[A], nodeKey: Key)(using CanEqual[A, A]): IO[Err, Unit] =
    node match
      case Node(_, y, _, false) if x == y => ZIO.unit

      case Node(_, y, _, true) if x == y =>
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

  def flatten[A: Codec](ns: String): Stream[Err, A] =
    for
      h <- ZStream.fromZIO(dba_head)
      xs <- flatten(ns, h)
    yield xs

  private def flatten[A: Codec](ns: String, nodeKey: Key): Stream[Err, A] =
    for
      node <- ZStream.fromZIO(dba_get(ns, nodeKey))
      xs <-
        node match
          case None => ZStream.empty
          case Some(Node(t, x, s, a)) =>
            val xs = ZStream(x).filter(_ => a)
            val ts = ZStream(t).collectSome.flatMap(flatten(ns, _))
            val ss = ZStream(s).collectSome.flatMap(flatten(ns, _))
            ts ++ xs ++ ss
    yield xs

  inline private def toNode[A](x: A): Node[A] = Node(None, x, None, active=true)

  type Key = Long
  
  case class Node[A](@N(1) left: Option[Key], @N(2) x: A, @N(3) right: Option[Key], @N(4) active: Boolean)
  
  def dba_add[A: Codec](ns: String, v: Node[A]): IO[Err, Key] =
    given CanEqual[None.type, Option[kvs.rng.Value]] = CanEqual.derived
    for
      nse <- encodeNS(ns)
      ide <- dba.get(nse)
      id <-
        ide match
          case Some(ide) => decodeKeyAsValue(ide)
          case None => ZIO.succeed(0L)
      id1 <- ZIO.succeed(id + 1L)
      id1e <- encodeKeyAsValue(id1)
      _ <- dba.put(nse, id1e)
      _ <- dba_put(ns, id1, v)
    yield id1
  
  def dba_put[A: Codec](ns: String, k: Key, v: Node[A]): IO[Err, Unit] =
    for
      ke <- encodeKey(ns, k)
      ve <- encodeNode(v)
      _ <- dba.put(ke, ve)
    yield ()
  
  def dba_get[A: Codec](ns: String, k: Key): IO[Err, Option[Node[A]]] =
    for
      ke <- encodeKey(ns, k)
      ve <- dba.get(ke)
      v <-
        ve match
          case None => ZIO.none
          case Some(ve) => decodeNode(ve).asSome
    yield v

  def dba_head: UIO[Key] = ZIO.succeed(1L)

  def encodeNode[A: Codec](x: Node[A]): UIO[Array[Byte]] =
    ZIO.succeed(encode(x))
  
  def decodeNode[A: Codec](bs: Array[Byte]): UIO[Node[A]] =
    ZIO.succeed(decode(bs))

  def decodeKeyAsValue(bs: Array[Byte]): IO[IOException, Key] =
    for
      cis <- ZIO.succeed(CodedInputStream.newInstance(bs).nn)
      k <- ZIO.attempt(cis.readUInt64).refineToOrDie[IOException]
    yield k
  
  def encodeKeyAsValue(k: Key): IO[IOException, Array[Byte]] =
    for
      _ <- ZIO.when(k <= 0)(ZIO.dieMessage("key is not positive"))
      size <- ZIO.succeed(CodedOutputStream.computeUInt64SizeNoTag(k))
      bs <- ZIO.succeed(new Array[Byte](size))
      cos <- ZIO.succeed(CodedOutputStream.newInstance(bs).nn)
      _ <- ZIO.attempt(cos.writeUInt64NoTag(k)).refineToOrDie[IOException]
    yield bs
  
  def encodeKey(ns: String, k: Key): IO[IOException, Array[Byte]] =
    for
      _ <- ZIO.when(k <= 0)(ZIO.dieMessage("key is not positive"))
      size <-
        ZIO.succeed(
          CodedOutputStream.computeStringSizeNoTag(ns)
        + 1
        + CodedOutputStream.computeUInt64SizeNoTag(k)
        )
      bs <- ZIO.succeed(new Array[Byte](size))
      cos <- ZIO.succeed(CodedOutputStream.newInstance(bs).nn)
      _ <- ZIO.attempt(cos.writeStringNoTag(ns)).refineToOrDie[IOException]
      _ <- ZIO.attempt(cos.write(0x9: Byte)).refineToOrDie[IOException]
      _ <- ZIO.attempt(cos.writeUInt64NoTag(k)).refineToOrDie[IOException]
    yield bs

  def encodeNS(ns: String): IO[IOException, Array[Byte]] =
    for
      size <- ZIO.succeed(CodedOutputStream.computeStringSizeNoTag(ns))
      bs <- ZIO.succeed(new Array[Byte](size))
      cos <- ZIO.succeed(CodedOutputStream.newInstance(bs).nn)
      _ <- ZIO.attempt(cos.writeStringNoTag(ns)).refineToOrDie[IOException]
    yield bs

  given [A: Codec]: Codec[Node[A]] = caseCodecAuto

  given ce1: CanEqual[None.type, Option[Node[?]]] = CanEqual.derived
  given ce2: CanEqual[None.type, Option[kvs.rng.Value]] = CanEqual.derived
end SortLive

object SortLive:
  val layer: URLayer[Dba, Sort] =
    ZLayer.fromFunction(SortLive(_))
