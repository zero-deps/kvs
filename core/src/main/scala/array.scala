package kvs

import zero.ext._, either._, option._, boolean._
import zd.proto._, api._, macrosapi._

import store.Dba

object array {
  case class Fd(@N(1) last: Long, @N(2) size: Long)
  case class Idx(@N(1) idx: Long)
  case class En(@N(1) data: Bytes)

  implicit val elkeyc = caseCodecAuto[ElKey]
  implicit val idxc = caseCodecAuto[Idx]
  implicit val fdc = caseCodecAuto[Fd]
  implicit val enc = caseCodecAuto[En]

  object meta {
    def put(id: FdKey, el: Fd)(implicit dba: Dba): Res[Unit] = dba.put(id, pickle(el))

    def get(id: FdKey)(implicit dba: Dba): Res[Option[Fd]] = dba.get(id) match {
      case Right(Some(x)) => unpickle[Fd](x).some.right
      case Right(None) => none.right
      case x@Left(_) => x.coerceRight
    }

    def delete(id: FdKey)(implicit dba: Dba): Res[Unit] = dba.delete(id)
  }

  private def key(fid: FdKey, idx: Long): EnKey = {
    EnKey(fid, ElKey(encodeToBytes(Idx(idx))))
  }

  def put(fid: FdKey, idx: Long, data: Bytes)(implicit dba: Dba): Res[Unit] = {
    for {
      fd <- meta.get(fid)
      sz  = Math.min(2L, fd.cata(_.size, idx))
      _  <- validate_size(sz)
      _  <- dba.put(key(fid, idx), pickle(En(data)))
      _  <- meta.put(fid, Fd(last=idx, size=sz))
    } yield ()
  }

  def add(fid: FdKey, size: Long, data: Bytes)(implicit dba: Dba): Res[Unit] = {
    for {
      _    <- validate_size(size)
      last <- meta.get(fid).map(_.cata(_.last, 0L))
      next  = ((last-1) % size)+1
      _    <- dba.put(key(fid, next), pickle(En(data)))
      _    <- meta.put(fid, Fd(last=next, size=size))
    } yield ()
  }

  def get(fid: FdKey)(idx: Long)(implicit dba: Dba): Res[Option[Bytes]] = {
    dba.get(key(fid, idx)) match {
      case Right(Some(x)) => unpickle[En](x).data.some.right
      case Right(None) => none.right
      case x@Left(_) => x.coerceRight
    }
  }

  def all(fid: FdKey)(implicit dba: Dba): Res[LazyList[Res[Bytes]]] = {
    for {
      size <- meta.get(fid).map(_.cata(_.size, 2L))
      m    <- meta.get(fid)
      last  = m.cata(_.last, 0L)
      xs    = if (last < size) LazyList.range(last+1, size) #::: LazyList.range(1, last)
              else LazyList.range(1, size)
    } yield xs.map(get(fid)).collect{
              case e@Left(_) => e.coerceRight
              case Right(Some(a)) => a.right
            }
  }

  private def validate_size(size: Long): Res[Unit] = {
    for {
      _ <- (size <= 0L).fold(Fail("size must be positivie").left, ().right)
      _ <- (size == 1L).fold(Fail("size is too small for array").left, ().right)
    } yield ()
  }
}
