package kvs.feed

import akka.actor.Actor
import zio.*, stream.*

import proto.*

import kvs.rng.{Dba, Key}

/*
 * Feed:
 * [head] -->next--> [en] -->next--> [nothing]
 */

private[feed] type Fid = String // feed id
type Eid = Long // entry id
private[feed] type Data = Array[Byte]
type Err = kvs.rng.AckQuorumFailed | kvs.rng.AckTimeoutFailed

private[feed] def pickle[A](e: A)(using MessageCodec[A]): UIO[Array[Byte]] = IO.effectTotal(encode[A](e))
private[feed] def unpickle[A](a: Array[Byte])(using MessageCodec[A]): UIO[A] = IO.effect(decode[A](a)).orDie // is defect

object ops:
  private[feed] case class Fd
    ( @N(1) head: Option[Eid]
    , @N(2) length: Long
    , @N(3) removed: Long
    , @N(4) maxid: Eid
    )

  private[feed] object Fd:
    val empty = Fd(head=None, length=0, removed=0, maxid=0)

  private[feed] case class En
    ( @N(1) next: Option[Eid]
    , @N(2) data: Data
    , @N(3) removed: Boolean = false
    )

  private[feed] given MessageCodec[Fd] = caseCodecAuto
  private[feed] given MessageCodec[En] = caseCodecAuto
  private[feed] given MessageCodec[(Fid, Eid)] = caseCodecIdx

  private[feed] object meta:
    def len(id: Fid)(dba: Dba.Service): IO[Err, Long] =
      get(id)(dba).map(_.fold(0L)(_.length))
    
    def delete(id: Fid)(dba: Dba.Service): IO[Err, Unit] =
      dba.delete(stob(id))
    
    def put(id: Fid, el: Fd)(dba: Dba.Service): IO[Err, Unit] =
      for
        p <- pickle(el)
        x <- dba.put(stob(id), p)
      yield x
    
    def get(id: Fid)(dba: Dba.Service): IO[Err, Option[Fd]] = 
      dba.get(stob(id)).flatMap{
        case Some(x) => unpickle[Fd](x).map(Some(_))
        case None => IO.succeed(None)
      }
  end meta

  private def _get(key: Key)(dba: Dba.Service): IO[Err, Option[En]] =
    dba.get(key).flatMap(_ match
      case Some(x) =>
        unpickle[En](x).map{
          case en if en.removed => None
          case en => Some(en)
        }
      case None => IO.succeed(None)
    )

  private[feed] def get(fid: Fid, eid: Eid)(dba: Dba.Service): IO[Err, Option[Data]] =
    for
      key <- pickle((fid, eid))
      x <- _get(key)(dba).map(_.map(_.data))
    yield x

  /**
  * Mark entry for removal. O(1) complexity.
  * @return true if marked for removal
  */
  private[feed] def remove(fid: Fid, eid: Eid)(dba: Dba.Service): IO[Err, Boolean] =
    for
      key <- pickle((fid, eid))
      en1 <- _get(key)(dba)
      res <-
        en1.fold(IO.succeed(false))(en =>
          for
            fd <- meta.get(fid)(dba).flatMap(_.fold(IO.dieMessage("feed is not exists"))(IO.succeed))
            _  <- meta.put(fid, fd.copy(length=fd.length-1, removed=fd.removed+1))(dba)
            p  <- pickle(en.copy(removed=true))
            _  <- dba.put(key, p)
          yield true
        )
    yield res

  /**
  * Adds the entry to the container. Creates the container if it's absent.
  * ID will be generated.
  */
  private[feed] def add(fid: Fid, data: Data)(dba: Dba.Service): IO[Err, Eid] =
    for {
      fd1 <- meta.get(fid)(dba)
      fd <- fd1.fold(meta.put(fid, Fd.empty)(dba).map(_ => Fd.empty))(IO.succeed)
      id = fd.maxid + 1
      en = En(next=fd.head, data=data)
      _ <- meta.put(fid, fd.copy(maxid=id))(dba) // in case kvs will fail after adding the en
      key <- pickle((fid, id))
      p <- pickle(en)
      _ <- dba.put(key, p)
      _ <- meta.put(fid, fd.copy(head=Some(id), length=fd.length+1, maxid=id))(dba)
    } yield id

  /* all items with removed starting from specified key */
  private def entries(fid: Fid, start: Eid)(dba: Dba.Service): Stream[Err, (Eid, En)] =
    Stream.
      unfoldM(Some(start): Option[Eid]){
        case None => IO.succeed(None)
        case Some(id) =>
          for
            k <- pickle((fid, id))
            bs <-
              dba.get(k).flatMap{
                case None => IO.dieMessage("feed is corrupted")
                case Some(bs) => IO.succeed(bs)
              }
            en <- unpickle[En](bs)
          yield Some((id -> en) -> en.next)
      }

  /* all items with removed starting from beggining */
  private def entries(fid: Fid)(dba: Dba.Service): Stream[Err, (Eid, En)] =
    Stream.fromEffect(meta.get(fid)(dba)).flatMap{
      case None => Stream.empty
      case Some(a) => a.head.fold(Stream.empty)(entries(fid, _)(dba))
    }

  /* all items without removed starting from specified key */
  private def entries_live(fid: Fid, eid: Eid)(dba: Dba.Service): Stream[Err, (Eid, En)] =
    entries(fid, eid)(dba).
      filterNot{ case (_,en) => en.removed }

  /* all items without removed starting from beggining */
  private def entries_live(fid: Fid)(dba: Dba.Service): Stream[Err, (Eid, En)] =
    entries(fid)(dba).
      filterNot{ case (_,en) => en.removed }

  /* all data without removed from beggining */
  private[feed] def all(fid: Fid)(dba: Dba.Service): Stream[Err, (Eid, Data)] =
    entries_live(fid)(dba).map{ case (id, en) => id -> en.data }

  /* all data without removed from specified key */
  private[feed] def all(fid: Fid, eid: Eid)(dba: Dba.Service): Stream[Err, (Eid, Data)] =
    entries_live(fid, eid)(dba).map{ case (id, en) => id -> en.data }

  /* delete all entries marked for removal, O(n) complexity */
  private[feed] def cleanup(fid: Fid)(dba: Dba.Service): IO[Err, Unit] =
    meta.get(fid)(dba).flatMap{
      case None => IO.unit
      case Some(fd) =>
        for
          /* remove from head */
          x <-
            entries(fid)(dba).
              takeWhile{ case (_, en) => en.removed }.
              mapM{ case (id, en) => 
                for
                  k <- pickle((fid, id))
                  _ <- dba.delete(k)
                yield en.next
              }.
              runLast
          /* fix head */
          _ <- x.fold(IO.unit)(id => meta.put(fid, fd.copy(head=id))(dba))
          /* remove in the middle */
          _ <-
            entries(fid)(dba).
              /* zip with stream of last live entry before removed ones */
              /* [1(live),2(removed),3(removed),4(live),5(removed)] becomes [1,1,1,4,4] */
              zip(entries(fid)(dba).scan(None: Option[(Eid,En)]){
                case (None, (id, en)) => Some(id->en)
                case (   x, ( _, en)) if en.removed => x
                case (   _, (id, en)) => Some(id->en)
              }.collect{ case Some(x) => x }).
              filter{ case (( _, en), _) => en.removed }.
              mapM{ case ((id, en), (id2, en2)) =>
                for
                  /* change link */
                  p <- pickle(en2.copy(next=en.next))
                  k2 <- pickle((fid, id2))
                  _ <- dba.put(k2, p)
                  /* delete entry */
                  k <- pickle((fid, id))
                  _ <- dba.delete(k)
                yield ()
              }.
              runDrain
          /* fix removed/maxid */
          _ <- fix(fid, fd)(dba)
        yield ()
    }

  /* fix length, removed and maxid for feed */
  private def fix(fid: Fid, fd: Fd)(dba: Dba.Service): IO[Err, Unit] =
    for
      x <-
        entries(fid)(dba).fold((0L, 0L, 0L)){
          case ((l, r, m), (id, en)) =>
            val l2 = en.removed.fold(l, l+1)
            val r2 = en.removed.fold(r+1, r)
            val m2 = Math.max(m, id)
            (l2, r2, m2)
        }
      (l, r, m) = x
      _ <- meta.put(fid, fd.copy(length=l, removed=r, maxid=m))(dba)
    yield ()

end ops

private[feed] inline def stob(s: String): Array[Byte] =
  s.getBytes("utf8").nn

extension (x: Boolean)
  private[feed] inline def fold[A](t: => A, f: => A): A =
    if x then t else f

given CanEqual[Nothing, kvs.rng.Value] = CanEqual.derived
given CanEqual[Nothing, (Eid, ops.En)] = CanEqual.derived
given CanEqual[Nothing, ops.Fd] = CanEqual.derived
