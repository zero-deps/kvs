package mws.kvs
package handle

import scalaz._, Scalaz._, Maybe.{Empty, Just}

import store._

object EnHandler {
  /**
   * Given EnHandler S create the EnHandler for A from conversion functions
   */
  def by[A, S](f: A => S)(g: S => A)(implicit h: EnHandler[S], _fh: FdHandler): EnHandler[A] = new EnHandler[A] {
    val fh = _fh

    def pickle(e: En[A]): Array[Byte] = h.pickle(en_A_to_En_S(e))
    def unpickle(a: Array[Byte]): Res[En[A]] = h.unpickle(a).map(en_S_to_En_A)

    private val en_A_to_En_S: En[A]=>En[S] = {
      case En(fid,id,prev,data) => En[S](fid,id,prev,f(data))
    }

    private val en_S_to_En_A: En[S]=>En[A] = {
      case En(fid,id,prev,data) => En[A](fid,id,prev,g(data))
    }
  }
}

trait EnHandler[T] extends EntryHandler[En[T]] {
  override protected def update(en: En[T], id: String, prev: String): En[T] = en.copy(id = id, prev = prev)
  override protected def update(en: En[T], prev: String): En[T] = en.copy(prev = prev)
}

/**
 * Abstract type entry handler
 * Since we don't know the exact type the pickler/unpickler still needs to be provided explicitly
 *
 * top --prev--> en --prev--> empty
 */
trait EntryHandler[A <: Entry] extends Handler[A] {
  val fh: FdHandler

  private def key(fid:String,id:String):String = s"${fid}.${id}"
  private def _put(en:A)(implicit dba:Dba):Res[A] = dba.put(key(en.fid,en.id),pickle(en)).map(_=>en)
  def get(fid:String,id:String)(implicit dba:Dba):Res[A] = dba.get(key(fid,id)).flatMap(unpickle)
  private def delete(fid:String,id:String)(implicit dba:Dba):Res[A] = dba.delete(key(fid,id)).flatMap(unpickle)

  protected def update(en: A, id: String, prev: String): A
  protected def update(en: A, prev: String): A

  /**
   * Adds the entry to the container
   * Creates the container if it's absent
   * @param en entry to add (prev is ignored). If id is empty it will be generated
   */
  def add(en: A)(implicit dba: Dba): Res[A] =
    fh.get(Fd(en.fid)).fold(
      l => fh.put(Fd(en.fid)), // create feed if it doesn't exist
      r => r.right
    ).flatMap{ fd: Fd =>
      ( if (en.id === empty)
          dba.nextid(en.fid) // generate ID if it is empty
        else
          get(en.fid, en.id).fold( // id of entry must be unique
            l => en.id.right,
            r => EntryExist(key(en.fid, en.id)).left
          )
      ).map(id => update(en, id=id, prev=fd.top)).flatMap{ en =>
        // add new entry with prev pointer
        _put(en).flatMap{ en =>
          // update feed's top
          fh.put(fd.copy(top=en.id, count=fd.count+1)).map(_ => en)
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
      l => add(en),
      r => _put(update(en, r.prev))
    )

  /**
   * Remove the entry from the container specified
   * @return deleted entry (with data)
   */
  def remove(_fid:String, _id:String)(implicit dba:Dba):Res[A] =
    // get entry to delete
    get(_fid, _id).flatMap{ en =>
      val id = en.id
      val fid = en.fid
      val prev = en.prev
      fh.get(Fd(fid)).flatMap{ fd =>
        val top = fd.top
        ( if (id == top)
            // change top and decrement
            fh.put(fd.copy(top=prev,count=fd.count-1))
          else
            // find entry which points to this one (next)
            Stream.iterate(start=get(fid,top))(_.flatMap(x=>get(fid,x.prev)))
              .takeWhile(_.isRight)
              .flatMap(_.toOption)
              .find(_.prev==id)
              .\/>(NotFound(key(fid, id)))
              .flatMap{ next =>
                // change link
                _put(update(next, prev=prev)).flatMap{ _ =>
                  // decrement count
                  fh.put(fd.copy(count=fd.count-1))
                }
              }
        ).flatMap(_ => delete(fid,id)) // delete entry
      }
    }

  /** Iterates through container and return the stream of entries.
   *
   * Stream is FILO ordered (most recent is first).
   * @param from if specified then return entries after this entry
   */
  def stream(fid:String,from:Maybe[A])(implicit dba:Dba):Res[Stream[A]] = {
    def _stream(start:String):Stream[A] =
      Stream.iterate(start=get(fid,start))(_.flatMap(x=>get(fid,x.prev)))
        .takeWhile(_.isRight)
        .flatMap(_.toOption)
    from match {
      case Empty() => fh.get(Fd(fid)).map(_.top).map{
        case `empty` => Stream.empty
        case start => _stream(start)
      }
      case Just(from) => from.prev match {
        case `empty` => Stream.empty.right
        case start => _stream(start).right
      }
    }
  }
}
