package kvs
package feed

import zd.rng.*
import proto.*
import zio.*, stream.*
import zio.ZIO.{succeed => pure}

type Eid = Long /* Entry ID */
type Fid = String /* Feed ID */

case class Fd
  ( @N(1) head: Option[Eid]
  )

case class En
  ( @N(1) next: Option[Eid]
  , @N(2) data: Array[Byte]
  , @N(3) removed: Boolean = false
  )

/* Abstract type feed: [head] -->next--> [en] -->next--> (nothing) */
trait Feed:
  /* All data without removed from beginning or specified key */
  def all[A: Codec](fid: Fid, eid: Option[Eid]=None): UStream[(Eid, A)]
  /* Get entry if exists and not removed from feed */
  def get[A: Codec](fid: Fid, eid: Eid): UIO[Option[A]]
  /* Adds the entry to the container. Creates the container if it's absent. ID will be generated */
  def add[A: Codec](fid: Fid, a: A): UIO[Eid]
  /* Mark entry as removed and delete its data. O(1) complexity */
  def remove(fid: Fid, eid: Eid): UIO[Unit]
end Feed

class FeedImpl(dba: Dba)(using
  CanEqual[None.type, Option[Value | Fd| (Eid, En)]]
, Codec[(Fid, Eid)]
, Codec[En]
, Codec[Fd]
) extends Feed:

  def all[A: Codec](fid: Fid, eid: Option[Eid]=None): UStream[(Eid, A)] =
    eid
      .fold {
        ZStream
          .fromZIO:
            dba.get(fid).some.flatMap(x => pure(decode[Fd](x))).unsome
          .collect:
            case Some(fd) => fd.head
      } {
        start => ZStream(Some(start))
      }
      .flatMap:
        ZStream
          .unfoldZIO(_):
            case None => ZIO.none
            case Some(id) =>
              pure(encode(fid -> id))
                .flatMap(dba.get)
                .someOrElseZIO(ZIO.dieMessage("feed is corrupted"))
                .flatMap(x => pure(decode[En](x)))
                .map(en => Some((id -> en) -> en.next))
          .collectZIO:
            case (id, En(_, data, false)) => pure(decode(data)).map(id -> _)

  def get[A: Codec](fid: Fid, eid: Eid): UIO[Option[A]] =
    (for
      key <- pure(encode(fid -> eid))
      v <- dba.get(key).some
      a <- pure(decode[En](v))
        .flatMap:
          case En(_, _, true) => ZIO.none
          case En(_, data, _) => pure(decode[A](data)).asSome
        .some
    yield a).unsome

  def add[A: Codec](fid: Fid, a: A): UIO[Eid] =
    for
      fd <- dba.get(fid).some.flatMap(x => pure(decode[Fd](x))).unsome.someOrElseZIO(
        pure(encode(Fd(head=None))).flatMap(dba.put(fid, _)).map(_ => Fd(head=None))
      )
      eid = fd.head.fold(1L)(_ + 1)
      _ <- pure(encode(a)).flatMap(data => pure(encode(fid -> eid) -> encode(En(next=fd.head, data=data)))).flatMap(dba.put(_, _))
      _ <- pure(encode(fd.copy(head=Some(eid)))).flatMap(dba.put(fid, _))
    yield eid

  def remove(fid: Fid, eid: Eid): UIO[Unit] =
    for
      key <- pure(encode(fid -> eid))
      _ <- dba.get(key)
        .some
        .flatMap(x => pure(decode[En](x)))
        .unsome
        .map(_.flatMap(en => if en.removed then None else Some(en)))
        .some
        .flatMap(en => pure(encode(en.copy(removed=true, data=Array.emptyByteArray))))
        .flatMap(dba.put(key, _))
        .unsome
        .someOrElse(())
    yield ()
end FeedImpl

def all[A: Codec](fid: Fid, eid: Option[Eid]=None): ZStream[Feed, Nothing, (Eid, A)] =
  ZStream.serviceWithStream(_.all(fid, eid))

def get[A: Codec](fid: Fid, eid: Eid): RIO[Feed, Option[A]] =
  ZIO.serviceWithZIO(_.get(fid, eid))

def add[A: Codec](fid: Fid, a: A): RIO[Feed, Eid] =
  ZIO.serviceWithZIO(_.add(fid, a))

def remove(fid: Fid, eid: Eid): RIO[Feed, Unit] =
  ZIO.serviceWithZIO(_.remove(fid, eid))

val layer: URLayer[Dba, Feed] =
  given CanEqual[None.type, Option[Fd | Value| (Eid, En)]] = CanEqual.derived
  given Codec[(Fid, Eid)] = caseCodecIdx
  given Codec[En] = caseCodecAuto
  given Codec[Fd] = caseCodecAuto
  ZLayer.fromFunction(FeedImpl(_))
