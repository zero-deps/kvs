package zd.kvs
package en

import scala.annotation.tailrec
import zero.ext._, either._, option._
import zd.kvs.store.Dba
import zd.proto.api.{N, MessageCodec}
import zd.proto.macrosapi.{caseCodecAuto}
import zd.proto.Bytes

final case class En
  ( @N(1) next: Option[Bytes]
  , @N(2) data: Bytes
  , @N(3) removed: Boolean=false
  )

final case class Key
  ( @N(1) fid: Bytes
  , @N(2) id: Bytes
  )

final case class IdEn(id: Bytes, en: En)

/**
 * Abstract type entry handler
 * Since we don't know the exact type the pickler/unpickler still needs to be provided explicitly
 * [head] -->next--> [en] -->next--> [nothing]
 */
object EnHandler {
  private val fh = FdHandler
  private implicit val encodec: MessageCodec[En] = caseCodecAuto[En]
  private implicit val keycodec: MessageCodec[Key] = caseCodecAuto[Key]

  private def key(fid: Bytes, id: Bytes): Bytes = pickle(Key(fid=fid, id=id))
  private def _put(fid: Bytes, id: Bytes, en: En)(implicit dba:Dba):Res[En] = {
    for {
      _ <- dba.put(key(fid, id), pickle(en))
    } yield en
  }
  def get(fid: Bytes, id: Bytes)(implicit dba: Dba): Res[Option[En]] = {
    dba.get(key(fid,id)) match {
      case Right(Some(x)) => unpickle[En](x).some.right
      case Right(None) => None.right
      case x@Left(_) => x.coerceRight
    }
  }
  private def _get(fid: Bytes, id: Option[Bytes])(implicit dba: Dba): Res[En] = {
    id match {
      case Some(id) => _get(fid, id)
      case None => Fail("id is empty").left
    }
  }
  private def _get(fid: Bytes, id: Bytes)(implicit dba: Dba): Res[En] = {
    val k = key(fid, id)
    dba.get(k) match {
      case Right(Some(x)) => unpickle[En](x).right
      case Right(None) => Fail(s"k=${k} is not exists").left
      case x@Left(_) => x.coerceRight
    }
  }
  private def delete(fid:Bytes,id:Bytes)(implicit dba:Dba):Res[Unit] = dba.delete(key(fid,id))

  /**
   * Adds the entry to the container. Creates the container if it's absent.
   * ID will be generated.
   */
  def prepend(fid: Bytes, data: Bytes)(implicit dba: Dba): Res[IdEn] = {
    for {
      fd1 <- fh.get(fid)
      fd <- fd1.cata(_.right, fh.put(Fd(fid)).map(_ => Fd(fid)))
      id = fd.maxid.increment()
      en = En(next=fd.head, data=data)
      _ <- fh.put(fd.copy(maxid=id)) // in case kvs will fail after adding the en
      _ <- _put(fid, id, en)
      _ <- fh.put(fd.copy(head=id.some, length=fd.length+1, maxid=id))
    } yield IdEn(id=id, en=en)
  }

  def head(fid: Bytes)(implicit dba: Dba): Res[Option[En]] = {
    fh.get(fid).flatMap{
      case None => none.right
      case Some(fd) if fd.head.isEmpty => none.right
      case Some(Fd(_, Some(top), _, _, _)) => get(fid, top)
    }
  }

  /**
   * Adds the entry to the container. Creates the container if it's absent.
   */
  def prepend(fid: Bytes, id: Bytes, data: Bytes)(implicit dba: Dba): Res[En] = {
    for {
      en1 <- get(fid, id)
      _ <- en1.cata(_ => EntryExists(fid, id).left, ().right)
      fd1 <- fh.get(fid)
      fd <- fd1.cata(_.right, fh.put(Fd(fid)).map(_ => Fd(fid)))
      en = En(next=fd.head, data=data)
      maxid = BytesExt.max(id, fd.maxid)
      _ <- fh.put(fd.copy(maxid=maxid)) // in case kvs will fail after adding the en
      _ <- _put(fid, id, en)
      _ <- fh.put(fd.copy(head=id.some, length=fd.length+1, maxid=maxid))
    } yield en
  }

  /**
   * Puts the entry to the container
   * If entry don't exists in containter create container and add it to the head
   * If entry exists in container, put it in the same place
   */
  def put(fid: Bytes, id: Bytes, data: Bytes)(implicit dba: Dba): Res[En] = {
    for {
      x <- get(fid, id)
      z <- x.cata(y => _put(fid, id, En(next=y.next, data=data)), prepend(fid=fid, id=id, data=data))
    } yield z
  }

  /**
   * Mark entry for removal. O(1) complexity.
   * @return marked entry (with data). Or `None` if element is absent.
   */
  def remove_soft(fid: Bytes, id: Bytes)(implicit dba: Dba): Res[Option[En]] = {
    for {
      en1 <- get(fid, id)
      _ <- en1.cata(en => {
        val next = en.next
        for {
          fd <- fh.get(fid).flatMap(_.toRight(Fail(s"feed=${fid} is not exists")))
          _ <- fh.put(fd.copy(length=fd.length-1, removed=fd.removed+1))
          _ <- _put(fid, id, en.copy(removed=true))
        } yield ()
      }, ().right)
    } yield en1
  }

  /**
   * Delete all entries marked for removal. O(n) complexity.
   */
  def cleanup(fid: Bytes)(implicit dba: Dba): Res[Unit] = {
    @tailrec def loop2(x1: IdEn)(x2: Res[IdEn], xs: LazyList[Res[IdEn]]): Res[Option[Bytes]] = {
      x2 match {
        case Left(l) => l.left
        case Right(y2) if !y2.en.removed => x2.map(_.en.next)
        case Right(y2) if  y2.en.removed =>
          val res = for {
            // change link
            _ <- _put(fid, x1.id, x1.en.copy(next=y2.en.next))
            // update feed
            fd <- fh.get(fid).flatMap(_.toRight(Fail(s"${fid} is not exists")))
            maxid = if (y2.id == fd.maxid) fd.maxid.decrement() else fd.maxid
            _ <- fh.put(fd.copy(removed=fd.removed-1, maxid=maxid))
            // delete entry
            _ <- delete(fid, y2.id)
          } yield ()
          res match {
            case Right(()) => 
              if (xs.isEmpty) none.right
              else loop2(x1)(xs.head, xs.tail)
            case Left(l) => l.left
          }
      }
    }
    @tailrec def loop(tail: LazyList[Res[IdEn]], athead: Boolean): Res[Unit] = {
      tail match {
        case xs if xs.isEmpty => ().right
        case Right(y) #:: ys if !y.en.removed && !athead && ys.isEmpty => ().right
        case Right(y) #:: ys if !y.en.removed &&  athead =>
          loop(ys, athead=false)
        case Right(y) #:: ys if  y.en.removed &&  athead =>
          val res = for {
            // update feed
            fd <- fh.get(fid).flatMap(_.toRight(Fail(s"${fid} is not exists")))
            maxid = if (y.id == fd.maxid) fd.maxid.decrement() else fd.maxid
            _ <- fh.put(Fd(id=fid, head=y.en.next, length=fd.length, removed=fd.removed-1, maxid=maxid))
            // delete entry
            _ <- delete(fid, y.id)
          } yield ()
          res match {
            case Right(()) => loop(ys, athead=true)
            case Left(l) => l.left
          }
        case  Left(y) #:: ys => y.left
        case _ #:: Left(y2) #:: ys => y2.left
        case Right(y1) #:: Right(y2) #:: ys if !y2.en.removed =>
          loop(y2.right #:: ys, athead=false)
        case Right(y1) #:: Right(y2) #:: ys if  y2.en.removed =>
          loop2(y1)(y2.right, ys).flatMap(next => all(fid, next=next.some, removed=true)) match {
            case Right(zs) => loop(tail=zs, athead=false)
            case Left(l) => l.left
          }
      }
    }
    for {
      xs <- all(fid=fid, next=none, removed=true)
      _ <- loop(xs, athead=true)
    } yield ()
  }

  /** 
   * Iterates through container and return the stream of entries.
   * Stream is FILO ordered (most recent is first).
   * @param next if specified then return entries after this entry
   * None - start with head; Some(None) - empty seq; Some(Some(id)) - start with id.
   */
  def all(fid: Bytes, next: Option[Option[Bytes]], removed: Boolean)(implicit dba: Dba): Res[LazyList[Res[IdEn]]] = {
    fh.get(fid).map(_.cata(fd => all(fd=fd, next=next, removed=removed), LazyList.empty))
  }

  def all(fd: Fd, next: Option[Option[Bytes]], removed: Boolean)(implicit dba: Dba): LazyList[Res[IdEn]] = {
    def _stream(id: Option[Bytes]): LazyList[Res[IdEn]] = {
      id match {
        case None => LazyList.empty
        case Some(id) =>
          val en = _get(fd.id, id)
          en match {
            case Right(e) if e.removed && !removed => _stream(e.next)
            case Right(e) => LazyList.cons(IdEn(id=id, en=e).right, _stream(e.next))
            case e@Left(_) => LazyList(e.coerceRight)
          }
      }
    }
    _stream(next.getOrElse(fd.head))
  }

  /**
   * Fix length, removed and maxid for feed.
   */
  def fix(fid: Bytes)(implicit dba: Dba): Res[((Long,Long),(Long,Long),(Bytes,Bytes))] = {
    @tailrec def loop(xs: LazyList[Res[IdEn]], acc: (Long, Long, Bytes)): Res[(Long, Long, Bytes)] = {
      xs match {
        case _ if xs.isEmpty => acc.right
        case (l@Left(_)) #:: _ => l.coerceRight
        case Right(x) #:: xs =>
          val length = if (x.en.removed) acc._1 else acc._1+1
          val removed = if (x.en.removed) acc._2+1 else acc._2
          val maxid = BytesExt.max(x.id, acc._3)
          loop(xs, (length, removed, maxid))
      }
    }
    for {
      fd <- fh.get(fid).flatMap(_.toRight(Fail(s"feed=${fid} is not exists")))
      acc <- loop(all(fd, next=none, removed=true), (0L,0L,BytesExt.Empty))
      length = acc._1
      removed = acc._2
      maxid = acc._3
      _ <- fh.put(fd.copy(length=length, removed=removed, maxid=maxid))
    } yield ((fd.length -> length), (fd.removed -> removed), (fd.maxid -> maxid))
  }
}
