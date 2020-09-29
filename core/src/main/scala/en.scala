package zd.kvs
package en

import scala.annotation.tailrec
import zero.ext._, either._, option._, traverse._
import zd.kvs.store.Dba
import zd.proto._, api._, macrosapi._

final case class En
  ( @N(1) next: Option[ElKey]
  , @N(2) data: Bytes
  , @N(3) removed: Boolean=false
  )

/**
 * Abstract type entry handler
 * Since we don't know the exact type the pickler/unpickler still needs to be provided explicitly
 * [head] -->next--> [en] -->next--> [nothing]
 */
object EnHandler {
  private val fh = FdHandler
  private implicit val enc = {
    implicit val elkeyc = caseCodecAuto[ElKey]
    caseCodecAuto[En]
  }

  private def _put(key: EnKey, en: En)(implicit dba: Dba): Res[Unit] = {
    for {
      _ <- dba.put(key, pickle(en))
    } yield ()
  }

  private def _get(key: EnKey)(implicit dba: Dba): Res[Option[En]] = {
    dba.get(key) match {
      case Right(Some(x)) => unpickle[En](x) match {
        case en if en.removed => none.right
        case en => en.some.right
      }
      case Right(None) => none.right
      case x@Left(_) => x.coerceRight
    }
  }

  /**
   * Puts the entry to the container
   * If entry don't exists in containter create container and add it to the head
   * If entry exists in container, put it in the same place
   */
  def put(key: EnKey, data: Bytes)(implicit dba: Dba): Res[Unit] = {
    for {
      x <- _get(key)
      _ <- x.cata(y => _put(key, En(next=y.next, data=data)), prepend(key, data))
    } yield ()
  }

  def get(key: EnKey)(implicit dba: Dba): Res[Option[Bytes]] = {
    _get(key).map(_.map(_.data))
  }

  def apply(key: EnKey)(implicit dba: Dba): Res[Bytes] = {
    get(key).flatMap(_.cata(_.right, Fail(s"key=$key is not exists").left))
  }

  /**
   * Mark entry for removal. O(1) complexity.
   * @return marked entry (with data). Or `None` if element is absent.
   */
  def remove(key: EnKey)(implicit dba: Dba): Res[Option[En]] = {
    for {
      en1 <- _get(key)
      _ <- en1.cata(en => {
        val next = en.next
        for {
          fd <- fh.get(key.fid).flatMap(_.toRight(Fail(s"feed=${key.fid} is not exists")))
          _ <- fh.put(key.fid, fd.copy(length=fd.length-1, removed=fd.removed+1))
          _ <- _put(key, en.copy(removed=true))
        } yield ()
      }, ().right)
    } yield en1
  }

  /**
   * Adds the entry to the container. Creates the container if it's absent.
   * ID will be generated.
   */
  def prepend(fid: FdKey, data: Bytes)(implicit dba: Dba): Res[ElKey] = {
    for {
      fd1 <- fh.get(fid)
      fd <- fd1.cata(_.right, fh.put(fid, Fd()).map(_ => Fd()))
      id = fd.maxid.increment()
      en = En(next=fd.head, data=data)
      _ <- fh.put(fid, fd.copy(maxid=id)) // in case kvs will fail after adding the en
      key = EnKey(fid, id)
      _ <- _put(key, en)
      _ <- fh.put(fid, fd.copy(head=id.some, length=fd.length+1, maxid=id))
    } yield id
  }

  /**
   * Adds the entry to the container. Creates the container if it's absent.
   */
  def prepend(key: EnKey, data: Bytes)(implicit dba: Dba): Res[Unit] = {
    for {
      en1 <- _get(key)
      _ <- en1.cata(_ => EntryExists(key).left, ().right)
      fd1 <- fh.get(key.fid)
      fd <- fd1.cata(_.right, fh.put(key.fid, Fd()).map(_ => Fd()))
      en = En(next=fd.head, data=data)
      maxid = max(key.id, fd.maxid)
      _ <- fh.put(key.fid, fd.copy(maxid=maxid)) // in case kvs will fail after adding the en
      _ <- _put(key, en)
      _ <- fh.put(key.fid, fd.copy(head=key.id.some, length=fd.length+1, maxid=maxid))
    } yield ()
  }

  def head(fid: FdKey)(implicit dba: Dba): Res[Option[(ElKey, Bytes)]] = {
    all(fid, next=none, removed=false).flatMap(_.headOption.sequence).map(_.map(x => x._1 -> x._2.data))
  }

  /** 
   * Iterates through container and return the stream of entries.
   * Stream is FILO ordered (most recent is first).
   * @param next if specified then return entries after this entry
   * None - start with head; Some(None) - empty seq; Some(Some(id)) - start with id.
   */
  def all(fid: FdKey, next: Option[Option[ElKey]], removed: Boolean)(implicit dba: Dba): Res[LazyList[Res[(ElKey, En)]]] = {
    lazy val _stream: Option[ElKey] => LazyList[Res[(ElKey, En)]] = {
      case None => LazyList.empty
      case Some(id) =>
        dba.get(EnKey(fid, id)).map(_.map(unpickle[En](_))) match {
          case Right(Some(e)) if e.removed && !removed => _stream(e.next)
          case Right(Some(e)) => LazyList.cons((id -> e).right, _stream(e.next))
          case Right(None) => LazyList(Fail(s"Feed is corrupted at id=$id").left)
          case e@Left(_) => LazyList(e.coerceRight)
        }
    }
    fh.get(fid).map(_.cata(fd => _stream(next.getOrElse(fd.head)), LazyList.empty))
  }

  /**
   * Delete all entries marked for removal. O(n) complexity.
   */
  def cleanup(fid: FdKey)(implicit dba: Dba): Res[Unit] = {
    @tailrec def loop2(x1: (ElKey,En))(x2: Res[(ElKey,En)], xs: LazyList[Res[(ElKey,En)]]): Res[Option[ElKey]] = {
      x2 match {
        case Left(l) => l.left
        case Right(y2) if !y2._2.removed => x2.map(_._2.next)
        case Right(y2) if  y2._2.removed =>
          val res = for {
            // change link
            _ <- _put(EnKey(fid, id=x1._1), x1._2.copy(next=y2._2.next))
            // update feed
            fd <- fh.get(fid).flatMap(_.toRight(Fail(s"${fid} is not exists")))
            maxid = if (y2._1 == fd.maxid) fd.maxid.decrement() else fd.maxid
            _ <- fh.put(fid, fd.copy(removed=fd.removed-1, maxid=maxid))
            // delete entry
            _ <- dba.delete(EnKey(fid, id=y2._1))
          } yield ()
          res match {
            case Right(()) => 
              if (xs.isEmpty) none.right
              else loop2(x1)(xs.head, xs.tail)
            case Left(l) => l.left
          }
      }
    }
    @tailrec def loop(tail: LazyList[Res[(ElKey,En)]], athead: Boolean): Res[Unit] = {
      tail match {
        case xs if xs.isEmpty => ().right
        case Right(y) #:: ys if !y._2.removed && !athead && ys.isEmpty => ().right
        case Right(y) #:: ys if !y._2.removed &&  athead =>
          loop(ys, athead=false)
        case Right(y) #:: ys if  y._2.removed &&  athead =>
          val res = for {
            // update feed
            fd <- fh.get(fid).flatMap(_.toRight(Fail(s"${fid} is not exists")))
            maxid = if (y._1 == fd.maxid) fd.maxid.decrement() else fd.maxid
            _ <- fh.put(fid, Fd(head=y._2.next, length=fd.length, removed=fd.removed-1, maxid=maxid))
            // delete entry
            _ <- dba.delete(EnKey(fid, id=y._1))
          } yield ()
          res match {
            case Right(()) => loop(ys, athead=true)
            case Left(l) => l.left
          }
        case  Left(y) #:: ys => y.left
        case _ #:: Left(y2) #:: ys => y2.left
        case Right(y1) #:: Right(y2) #:: ys if !y2._2.removed =>
          loop(y2.right #:: ys, athead=false)
        case Right(y1) #:: Right(y2) #:: ys if  y2._2.removed =>
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
   * Fix length, removed and maxid for feed.
   */
  def fix(fid: FdKey)(implicit dba: Dba): Res[((Long,Long),(Long,Long),(ElKey,ElKey))] = {
    @tailrec def loop(xs: LazyList[Res[(ElKey,En)]], acc: (Long, Long, ElKey)): Res[(Long, Long, ElKey)] = {
      xs match {
        case _ if xs.isEmpty => acc.right
        case (l@Left(_)) #:: _ => l.coerceRight
        case Right(x) #:: xs =>
          val length = if (x._2.removed) acc._1 else acc._1+1
          val removed = if (x._2.removed) acc._2+1 else acc._2
          val maxid = max(x._1, acc._3)
          loop(xs, (length, removed, maxid))
      }
    }
    for {
      fd <- fh.get(fid).flatMap(_.toRight(Fail(s"feed=${fid} is not exists")))
      xs <- all(fid, next=none, removed=true)
      acc <- loop(xs, (0L, 0L, ElKey(Bytes.empty)))
      (length, removed, maxid) = acc
      _ <- fh.put(fid, fd.copy(length=length, removed=removed, maxid=maxid))
    } yield ((fd.length -> length), (fd.removed -> removed), (fd.maxid -> maxid))
  }

  private def max(x: ElKey, y: ElKey): ElKey = {
    import java.util.Arrays
    if (Arrays.compare(x.bytes.unsafeArray, y.bytes.unsafeArray) > 0) x else y
  }
}
