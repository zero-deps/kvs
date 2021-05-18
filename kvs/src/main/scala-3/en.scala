package zd.kvs
package en

import zd.kvs.store.Dba

trait En {
  val fid: String
  val id: String
  val prev: String
}

/**
 * Abstract type entry handler
 * Since we don't know the exact type the pickler/unpickler still needs to be provided explicitly
 *
 * [top] -->prev--> [en] -->prev--> [empty]
 */
trait EnHandler[A <: En] {
  val fh: FdHandler

  def pickle(e: A): Res[Array[Byte]]
  def unpickle(a: Array[Byte]): Res[A]

  private def key(fid:String,id:String):String = s"${fid}.${id}"
  private def _put(en:A)(implicit dba:Dba):Res[A] = pickle(en).flatMap(x => dba.put(key(en.fid,en.id),x)).map(_=>en)
  def get(fid: String, id: String)(implicit dba: Dba): Res[Option[A]] = {
    dba.get(key(fid,id)) match {
      case Right(Some(x)) => unpickle(x).map(Some(_))
      case Right(None) => Right(None)
      case x@Left(_) => x.coerceRight
    }
  }
  private def get1(fid: String, id: String)(implicit dba: Dba): Res[A] = {
    val k = key(fid,id)
    dba.get(k) match {
      case Right(Some(x)) => unpickle(x)
      case Right(None) => Left(NotFound(k))
      case x@Left(_) => x.coerceRight
    }
  }
  private def delete(fid:String,id:String)(implicit dba:Dba):Res[Unit] = dba.delete(key(fid,id))

  protected def update(en: A, id: String, prev: String): A
  protected def update(en: A, prev: String): A

  def head(fid: String)(implicit dba: Dba): Res[Option[A]] = {
    fh.get(Fd(fid)).flatMap{
      case None => Right(None)
      case Some(Fd(_, `empty`, _)) => Right(None)
      case Some(Fd(_, top, _)) => get(fid, top)
    }
  }

  /**
   * Adds the entry to the container
   * Creates the container if it's absent
   * @param en entry to add (prev is ignored). If id is empty it will be generated
   */
  def add(en: A)(implicit dba: Dba): Res[A] = {
    fh.get(Fd(en.fid)).flatMap(_.cata(Right(_), fh.put(Fd(en.fid)))).flatMap{ (fd: Fd) =>
      ( if (en.id == empty)
          dba.nextid(en.fid) // generate ID if it is empty
        else
          get(en.fid, en.id).flatMap( // id of entry must be unique
            _.cata(_ => Left(EntryExists(key(en.fid, en.id))), Right(en.id))
          )
      ).map(id => update(en, id=id, prev=fd.top)).flatMap{ en =>
        // add new entry with prev pointer
        _put(en).flatMap{ en =>
          // update feed's top
          fh.put(fd.copy(top=en.id, count=fd.count+1)).map(_ => en)
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
  def put(en: A)(implicit dba: Dba): Res[A] =
    get(en.fid, en.id).fold(
      l => Left(l),
      r => r.cata(x => _put(update(en, x.prev)), add(en))
    )

  def removeAfter(en: A, cleanup: A => Res[Unit])(implicit dba: Dba): Res[Unit] = {
    @annotation.tailrec def loop(fd: Fd, en: A, previd: String): Res[Unit] = {
      get(fd.id, previd) match {
        case l@Left(_) => l.coerceRight
        case Right(None) => Right(())
        case Right(Some(prev)) =>
          val fd1 = fd.copy(count=fd.count-1)
          (for {
            // change link
            _ <- _put(update(en, prev.prev))
            // decrement count
            _ <- fh.put(fd1)
            // delete prev
            _ <- delete(fd.id, prev.id)
            // additional actions after entry removed
            _ <- cleanup(prev)
          } yield ()) match {
            case l@Left(_) => l.coerceRight
            case Right(_) => loop(fd1, en, prev.prev)
          }
      }
    }
    fh.get(Fd(en.fid)).flatMap(_.cata(fd => loop(fd, en, en.prev), Left(NotFound(s"fid ${en.fid}"))))
  }

  def clearFeed(fid: String)(implicit dba: Dba): Res[Unit] = {
    for {
      en <- head(fid)
      _ <- en match {
          case Some(x) => for {
            _ <- removeAfter(x, (_: A) => Right(()))
            _ <- remove(fid, x.id)
          } yield ()
          case None => Right(())
        }
      _ <- fh.delete(Fd(fid))
    } yield ()
  }

  /**
   * Remove the entry from the container specified
   * @return deleted entry (with data)
   */
  def remove(_fid: String, _id: String)(implicit dba: Dba): Res[A] =
    // get entry to delete
    get1(_fid, _id).flatMap{ en =>
      val id = en.id
      val fid = en.fid
      val prev = en.prev
      fh.get(Fd(fid)).flatMap(_.cata(Right(_), Left(NotFound(fid)))).flatMap{ fd =>
        val top = fd.top
        ( if (id == top)
            // change top and decrement
            fh.put(fd.copy(top=prev,count=fd.count-1))
          else
            // find entry which points to this one (next)
            LazyList.iterate(start=get1(fid,top))(_.flatMap(x=>get1(fid,x.prev)))
              .takeWhile(_.isRight)
              .flatMap(_.toOption)
              .find(_.prev==id)
              .toRight(NotFound(key(fid, id)))
              .flatMap{ next =>
                // change link
                _put(update(next, prev=prev)).flatMap{ _ =>
                  // decrement count
                  fh.put(fd.copy(count=fd.count-1))
                }
              }
        ).flatMap(_ => delete(fid, id)). // delete entry
        map(_ => en) // return deleted entry
      }
    }

  /**
   * Remove the entries from the container specified
   * @return deleted entries (with data)
   */
  def remove(_fid: String, _ids: Seq[String])(implicit dba: Dba): Res[Vector[A]] = {
    val initIds = _ids.toSet
    @annotation.tailrec def loop(fd: Fd, en: A, ids: Set[String], acc: Vector[A]): Res[(Set[String], Vector[A])] = {
      if (en.prev == empty || ids.isEmpty)
        Right((ids, acc))
      else
        get1(fd.id, en.prev) match {
          case l@Left(_) => l.coerceRight
          case Right(prev) =>
            if (ids.contains(prev.id)) {
              val fd1 = fd.copy(count=fd.count-1)
              (for {
                // change link
                updated <- _put(update(en, prev.prev))
                // decrement count
                _ <- fh.put(fd1)
                // delete prev
                _ <- delete(fd.id, prev.id)
              } yield updated) match {
                case l@Left(_) => l.coerceRight
                case Right(updated) => loop(fd1, updated, ids - prev.id, acc :+ prev)
              }
            } else loop(fd, prev, ids, acc)
        }
    }

    for {
      _ <- _ids.isEmpty.fold(Left(InvalidArgument("ids can't be empty")), Right(()))
      fd <- fh.get(Fd(_fid)).flatMap(_.toRight(NotFound(s"fid ${_fid}")))
      en <- head(_fid)
      res <- en match {
        case Some(en) => for {
          tpl <- loop(fd, en, initIds, Vector())
          (ids, removed) = tpl
          tpl1 <-
            if (initIds.contains(en.id)) remove(_fid, en.id).map(rem => (ids - en.id, rem +: removed))
            else Right((ids, removed))
          (ids1, removed1) = tpl1
          res <- if (ids1.isEmpty) Right(removed1) else Left(NotFound(s"fid ${_fid} ids ${initIds.diff(ids1).mkString(" ")}"))
        } yield res
        case None => Left(NotFound(s"fid ${_fid} ids ${initIds.mkString(" ")}"))
      }
    } yield res
  }

  /** Iterates through container and return the stream of entries.
   *
   * Stream is FILO ordered (most recent is first).
   * @param from if specified then return entries after this entry
   */
  def all(fid: String, from: Option[A])(implicit dba: Dba): Res[LazyList[Res[A]]] = {
    def _stream(id: String): LazyList[Res[A]] = {
      id match {
        case `empty` => LazyList.empty
        case _ =>
          val en = get1(fid, id)
          en match {
            case Right(e) => LazyList.cons(en, _stream(e.prev))
            case _ => LazyList(en)
          }
      }
    }
    from match {
      case None => fh.get(Fd(fid)).map(_.cata(x => _stream(x.top), LazyList.empty))
      case Some(en) => Right(_stream(en.prev))
    }
  }
}
