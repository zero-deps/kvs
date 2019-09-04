package zd.kvs
package en

import zd.kvs.store.Dba
import zd.gs.z._

trait En {
  val fid: String
  val id_opt: Option[String]
  val prev_opt: Option[String]
}

/**
 * Abstract type entry handler
 * Since we don't know the exact type the pickler/unpickler still needs to be provided explicitly
 *
 * [top] -->prev--> [en] -->prev--> [none]
 */
trait EnHandler[A <: En] {
  val fh: FdHandler

  def pickle(e: A): Res[Array[Byte]]
  def unpickle(a: Array[Byte]): Res[A]

  private def key(fid:String,id:String):String = s"${fid}.${id}"
  private def _put(en:A)(implicit dba:Dba):Res[A] = {
    for {
      id <- en.id_opt.toRight(Fail("id is empty"))
      p <- pickle(en)
      _ <- dba.put(key(en.fid, id), p)
    } yield en
  }
  def get(fid: String, id: String)(implicit dba: Dba): Res[Option[A]] = {
    dba.get(key(fid,id)) match {
      case Right(Some(x)) => unpickle(x).map(_.just)
      case Right(None) => None.right
      case x@Left(_) => x.coerceRight
    }
  }
  private def _get(fid: String, id: Option[String])(implicit dba: Dba): Res[A] = {
    id match {
      case Some(id) => _get(fid, id)
      case None => Fail("id is empty").left
    }
  }
  private def _get(fid: String, id: String)(implicit dba: Dba): Res[A] = {
    val k = key(fid, id)
    dba.get(k) match {
      case Right(Some(x)) => unpickle(x)
      case Right(None) => Fail(k).left
      case x@Left(_) => x.coerceRight
    }
  }
  private def delete(fid:String,id:String)(implicit dba:Dba):Res[Unit] = dba.delete(key(fid,id))

  protected def update(en: A, id: Option[String], prev: Option[String]): A
  protected def update(en: A, prev: Option[String]): A

  /**
   * Adds the entry to the container
   * Creates the container if it's absent
   * @param en entry to add (prev is ignored). If id is empty it will be generated
   */
  def add(en: A)(implicit dba: Dba): Res[A] = {
    fh.get(Fd(en.fid)).flatMap(_.cata(_.right, fh.put(Fd(en.fid)).map(_ => Fd(en.fid)))).flatMap{ fd: Fd =>
      ( if (en.id_opt.isEmpty)
          dba.nextid(en.fid) // generate ID if it is empty
        else
          for {
            id <- en.id_opt.toRight(Fail("id is empty"))
            x <- get(en.fid, id)
            _ <- x.cata(_ => EntryExists(key(en.fid, id)).left, ().right)
          } yield id
      ).map(id => update(en, id=id.just, prev=fd.top_opt)).flatMap{ en =>
        // add new entry with prev pointer
        _put(en).flatMap{ _ =>
          // update feed's top
          fh.put(fd.copy(top_opt=en.id_opt, count=fd.count+1)).map(_ => en)
        }
      }
    }
  }

  /**
   * Puts the entry to the container
   * If entry don't exists in containter create container and add it to the head
   * If entry exists in container, put it in the same place
   * @param en entry to put (prev is ignored)
   */
  def put(en: A)(implicit dba: Dba): Res[A] = {
    for {
      id <- en.id_opt.toRight(Fail("id is empty"))
      x <- get(en.fid, id)
      z <- x.cata(y => _put(update(en, y.prev_opt)), add(en))
    } yield z
  }

  /**
   * Remove the entry from the container specified.
   * @return deleted entry (with data). Or None if element is absent.
   */
  def remove_opt(_fid: String, _id: String)(implicit dba: Dba): Res[Option[A]] = {
    for {
      en1 <- get(_fid, _id)
      _ <- en1.cata(en => {
        val fid = en.fid
        val id = _id
        val prev = en.prev_opt
        for {
          fd1 <- fh.get(Fd(fid))
          fd <- fd1.cata(_.right, Fail(fid).left)
          top = fd.top_opt
          _ <- if (Option(id) == top) {
            fh.put(fd.copy(top_opt=prev, count=fd.count-1))
          } else {
            for {
              next <- LazyList.iterate(start=_get(fid,top))(_.flatMap(x=>_get(fid,x.prev_opt))).
                takeWhile(_.isRight).
                flatMap(_.toOption).
                find(_.prev_opt == Option(id)).
                toRight(Fail(key(fid, id)))
              _ <- _put(update(next, prev=prev))
              _ <- fh.put(fd.copy(count=fd.count-1))
            } yield ()
          }
          _ <- delete(fid, id)
        } yield ()
      }, ().right)
    } yield en1
  }

  /** Iterates through container and return the stream of entries.
   *
   * Stream is FILO ordered (most recent is first).
   * @param from if specified then return entries after this entry
   */
  def all(fid: String, from: Option[A])(implicit dba: Dba): Res[LazyList[Res[A]]] = {
    def _stream(id: Option[String]): LazyList[Res[A]] = {
      id match {
        case None => LazyList.empty
        case Some(id) =>
          val en = _get(fid, id)
          en match {
            case Right(e) => LazyList.cons(en, _stream(e.prev_opt))
            case _ => LazyList(en)
          }
      }
    }
    from match {
      case None => fh.get(Fd(fid)).map(_.cata(x => _stream(x.top_opt), LazyList.empty))
      case Some(en) => _stream(en.prev_opt).right
    }
  }
}
