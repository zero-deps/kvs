package kvs

import zero.ext._, option._
import proto._, macrosapi._
import zio.{ZIO, IO} 
import zio.stream._

import store.Dba

/* https://en.wikipedia.org/wiki/Circular_buffer */
object circular {
  case class Fd(@N(1) last: Long, @N(2) size: Long)
  case class Idx(@N(1) idx: Long)
  case class En(@N(1) data: Bytes)

  implicit val elkeyc = caseCodecAuto[ElKey]
  implicit val idxc = caseCodecAuto[Idx]
  implicit val fdc = caseCodecAuto[Fd]
  implicit val enc = caseCodecAuto[En]

  object meta {
    def del(id: FdKey        )(implicit dba: Dba): IO[Err, Unit] = dba.del(id)
    def put(id: FdKey, el: Fd)(implicit dba: Dba): IO[Err, Unit] =
      for {
        p <- pickle(el)
        x <- dba.put(id, p)
      } yield x
    def get(id: FdKey        )(implicit dba: Dba): IO[Err, Option[Fd]] =
      dba.get(id).flatMap{
        case Some(x) => unpickle[Fd](x).map(_.some)
        case None    => IO.succeed(none)
      }
  }

  private def key(fid: FdKey, idx: Long): EnKey = {
    EnKey(fid, ElKey(encodeToBytes(Idx(idx))))
  }

  def put(fid: FdKey, idx: Long, data: Bytes)(implicit dba: Dba): IO[Err, Unit] = {
    for {
      fd <- meta.get(fid)
      sz  = Math.max(2L, fd.cata(_.size, idx))
      p  <- pickle(En(data))
      _  <- dba.put(key(fid, idx), p)
      _  <- meta.put(fid, Fd(last=idx, size=sz))
    } yield ()
  }

  def add(fid: FdKey, size1: Long, data: Bytes)(implicit dba: Dba): IO[Err, Unit] = {
    for {
      m    <- meta.get(fid)
      size  = Math.max(2L, size1)
      next  = m.cata(m => (m.last % size)+1, 1L)
      p    <- pickle(En(data))
      _    <- dba.put(key(fid, next), p)
      _    <- meta.put(fid, Fd(last=next, size=size))
    } yield ()
  }

  def get(fid: FdKey)(idx: Long)(implicit dba: Dba): IO[Err, Option[Bytes]] = {
    for {
      x <- dba.get(key(fid, idx))
      y <- x.cata(unpickle[En](_).map(_.data.some), IO.succeed(none))
    } yield y
  }

  def all(fid: FdKey)(implicit dba: Dba): Stream[Err, Bytes] = {
    val res = for {
      m  <- meta.get(fid)
    } yield m.cata(m =>
              if (m.last < m.size) LazyList.range(m.last+1, m.size+1) #::: LazyList.range(1, m.last+1)
              else LazyList.range(1, m.size+1)
            , LazyList.empty)
    ZStream.fromIterableM(res).mapM(get(fid)).collect{ case Some(x) => x }
  }
}
