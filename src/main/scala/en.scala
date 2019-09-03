package zd.kvs
package en

import zd.kvs.store.Dba
import zd.gs.z._

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
      case Right(Some(x)) => unpickle(x).map(_.just)
      case Right(None) => None.right
      case x@Left(_) => x.coerceRight
    }
  }
  private def get1(fid: String, id: String)(implicit dba: Dba): Res[A] = {
    val k = key(fid,id)
    dba.get(k) match {
      case Right(Some(x)) => unpickle(x)
      case Right(None) => NotFound(k).left
      case x@Left(_) => x.coerceRight
    }
  }
  private def delete(fid:String,id:String)(implicit dba:Dba):Res[Unit] = dba.delete(key(fid,id))

  protected def update(en: A, id: String, prev: String): A
  protected def update(en: A, prev: String): A

  /**
   * Adds the entry to the container
   * Creates the container if it's absent
   * @param en entry to add (prev is ignored). If id is empty it will be generated
   */
  def add(en: A)(implicit dba: Dba): Res[A] = {
    fh.get(Fd(en.fid)).flatMap(_.cata(_.right, fh.put(Fd(en.fid)))).flatMap{ fd: Fd =>
      ( if (en.id == empty)
          dba.nextid(en.fid) // generate ID if it is empty
        else
          get(en.fid, en.id).flatMap( // id of entry must be unique
            _.cata(_ => EntryExists(key(en.fid, en.id)).left, en.id.right)
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
      l => l.left,
      r => r.cata(x => _put(update(en, x.prev)), add(en))
    )

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
      fh.get(Fd(fid)).flatMap(_.cata(_.right, NotFound(fid).left)).flatMap{ fd =>
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

  /** Iterates through container and return the stream of entries.
   *
   * Stream is FILO ordered (most recent is first).
   * @param from if specified then return entries after this entry
   */
  def stream(fid: String, from: Option[A])(implicit dba: Dba): Res[LazyList[Res[A]]] = {
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
      case None => fh.get(Fd(fid)).flatMap(_.cata(_.right, NotFound(fid).left)).map(r => _stream(r.top))
      case Some(en) => _stream(en.prev).right
    }
  }
}
