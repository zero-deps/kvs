package zd.kvs
package en

import scala.annotation.tailrec
import zd.gs.z._
import zd.kvs.store.Dba
import zd.proto.api.{N, MessageCodec}
import zd.proto.macrosapi.{caseCodecAuto}
import zd.proto.Bytes

final case class En
  ( @N(1) id: Bytes //todo: delete
  , @N(2) next: Option[Bytes]
  , @N(3) data: Bytes
  , @N(4) removed: Boolean=false
  )

final case class Key
  ( @N(1) fid: Bytes
  , @N(2) id: Bytes
  )

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
  private def _put(fid: Bytes, en: En)(implicit dba:Dba):Res[En] = {
    for {
      _ <- dba.put(key(fid, en.id), pickle(en))
    } yield en
  }
  def get(fid: Bytes, id: Bytes)(implicit dba: Dba): Res[Option[En]] = {
    dba.get(key(fid,id)) match {
      case Right(Some(x)) => unpickle[En](x).map(_.just)
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
      case Right(Some(x)) => unpickle(x)
      case Right(None) => Fail(s"k=${k} is not exists").left
      case x@Left(_) => x.coerceRight
    }
  }
  private def delete(fid:Bytes,id:Bytes)(implicit dba:Dba):Res[Unit] = dba.delete(key(fid,id))

  /**
   * Adds the entry to the container. Creates the container if it's absent.
   * ID will be generated.
   */
  def prepend(fid: Bytes, data: Bytes)(implicit dba: Dba): Res[En] = {
    for {
      fd1 <- fh.get(fid)
      fd <- fd1.cata(_.right, fh.put(Fd(fid)).map(_ => Fd(fid)))
      id = fd.maxid.increment()
      en = En(id=id, next=fd.head, data=data)
      _ <- fh.put(fd.copy(maxid=id)) // in case kvs will fail after adding the en
      _ <- _put(fid, en)
      _ <- fh.put(fd.copy(head=id.just, length=fd.length+1, maxid=id))
    } yield en
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
      en = En(id=id, next=fd.head, data=data)
      maxid = BytesExt.max(id, fd.maxid)
      _ <- fh.put(fd.copy(maxid=maxid)) // in case kvs will fail after adding the en
      _ <- _put(fid, en)
      _ <- fh.put(fd.copy(head=id.just, length=fd.length+1, maxid=maxid))
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
      z <- x.cata(y => _put(fid, En(id=id, next=y.next, data=data)), prepend(fid=fid, id=id, data=data))
    } yield z
  }

  /**
   * Mark entry for removal. O(1) complexity.
   * @return marked entry (with data). Or `Nothing` if element is absent.
   */
  def remove_soft(fid: Bytes, id: Bytes)(implicit dba: Dba): Res[Option[En]] = {
    for {
      en1 <- get(fid, id)
      _ <- en1.cata(en => {
        val next = en.next
        for {
          fd <- fh.get(fid).flatMap(_.toRight(Fail(s"feed=${fid} is not exists")))
          _ <- fh.put(fd.copy(length=fd.length-1, removed=fd.removed+1))
          _ <- _put(fid, en.copy(removed=true))
        } yield ()
      }, ().right)
    } yield en1
  }

  /**
   * Delete all entries marked for removal. O(n) complexity.
   */
  def cleanup(fid: Bytes)(implicit dba: Dba): Res[Unit] = {
    @tailrec def loop2(x1: En)(x2: Res[En], xs: LazyList[Res[En]]): Res[Option[Bytes]] = {
      x2 match {
        case Left(l) => l.left
        case Right(y2) if !y2.removed => x2.map(_.next)
        case Right(y2) if  y2.removed =>
          val res = for {
            // change link
            _ <- _put(fid, x1.copy(next=y2.next))
            // update feed
            fd <- fh.get(fid).flatMap(_.toRight(Fail(s"${fid} is not exists")))
            maxid = if (y2.id == fd.maxid) fd.maxid.decrement() else fd.maxid
            _ <- fh.put(fd.copy(removed=fd.removed-1, maxid=maxid))
            // delete entry
            _ <- delete(fid, y2.id)
          } yield ()
          res match {
            case Right(()) => 
              if (xs.isEmpty) Nothing.right
              else loop2(x1)(xs.head, xs.tail)
            case Left(l) => l.left
          }
      }
    } 
    @tailrec def loop(tail: LazyList[Res[En]], athead: Boolean): Res[Unit] = {
      tail match {
        case xs if xs.isEmpty => ().right
        case Right(y) #:: ys if !y.removed && !athead && ys.isEmpty => ().right
        case Right(y) #:: ys if !y.removed &&  athead =>
          println(s"skip y=${y} at head")
          loop(ys, athead=false)
        case Right(y) #:: ys if  y.removed &&  athead =>
          println(s"delete ${y.id} at head")
          println(s"feed.head := ${y.next}")
          val res = for {
            // update feed
            fd <- fh.get(fid).flatMap(_.toRight(Fail(s"${fid} is not exists")))
            maxid = if (y.id == fd.maxid) fd.maxid.decrement() else fd.maxid
            _ <- fh.put(Fd(id=fid, head=y.next, length=fd.length, removed=fd.removed-1, maxid=maxid))
            // delete entry
            _ <- delete(fid, y.id)
          } yield ()
          res match {
            case Right(()) => loop(ys, athead=true)
            case Left(l) => l.left
          }
        case  Left(y) #:: ys => y.left
        case _ #:: Left(y2) #:: ys => y2.left
        case Right(y1) #:: Right(y2) #:: ys if !y2.removed =>
          println(s"y1=${y1} y2=${y2}")
          loop(y2.right #:: ys, athead=false)
        case Right(y1) #:: Right(y2) #:: ys if  y2.removed =>
          loop2(y1)(y2.right, ys).flatMap(next => all(fid, next=next.just, removed=true)) match {
            case Right(zs) => loop(tail=zs, athead=false)
            case Left(l) => l.left
          }
      }
    }
    for {
      xs <- all(fid=fid, next=Nothing, removed=true)
      _ <- loop(xs, athead=true)
    } yield ()
  }

  /** 
   * Iterates through container and return the stream of entries.
   * Stream is FILO ordered (most recent is first).
   * @param next if specified then return entries after this entry
   * Nothing - start with head; Just(Nothing) - empty seq; Just(Just(id)) - start with id.
   */
  def all(fid: Bytes, next: Maybe[Maybe[Bytes]], removed: Boolean)(implicit dba: Dba): Res[LazyList[Res[En]]] = {
    fh.get(fid).map(_.cata(fd => all(fd=fd, next=next, removed=removed), LazyList.empty))
  }

  def all(fd: Fd, next: Maybe[Maybe[Bytes]], removed: Boolean)(implicit dba: Dba): LazyList[Res[En]] = {
    def _stream(id: Maybe[Bytes]): LazyList[Res[En]] = {
      id match {
        case None => LazyList.empty
        case Some(id) =>
          val en = _get(fd.id, id)
          en match {
            case Right(e) if e.removed && !removed => _stream(e.next)
            case Right(e) => LazyList.cons(e.right, _stream(e.next))
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
    @tailrec def loop(xs: LazyList[Res[En]], acc: (Long, Long, Bytes)): Res[(Long, Long, Bytes)] = {
      xs match {
        case _ if xs.isEmpty => acc.right
        case (l@Left(_)) #:: _ => l.coerceRight
        case Right(x) #:: xs =>
          val length = if (x.removed) acc._1 else acc._1+1
          val removed = if (x.removed) acc._2+1 else acc._2
          val maxid = BytesExt.max(x.id, acc._3)
          loop(xs, (length, removed, maxid))
      }
    }
    for {
      fd <- fh.get(fid).flatMap(_.toRight(Fail(s"feed=${fid} is not exists")))
      acc <- loop(all(fd, next=Nothing, removed=true), (0L,0L,BytesExt.Empty))
      length = acc._1
      removed = acc._2
      maxid = acc._3
      _ <- fh.put(fd.copy(length=length, removed=removed, maxid=maxid))
    } yield ((fd.length -> length), (fd.removed -> removed), (fd.maxid -> maxid))
  }
}
