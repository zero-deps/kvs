package mws.kvs
package handle

import scala.annotation.tailrec
import store._

object EnHandler {
  /**
   * Given EnHandler S create the EnHandler for A from conversion functions
   */
  def by[A, S](f: A => S)(g: S => A)(implicit h: EnHandler[S]): EnHandler[A] = new EnHandler[A] {
    def pickle(e: En[A]): Array[Byte] = h.pickle(en_A_to_En_S(e))
    def unpickle(a: Array[Byte]): En[A] = en_S_to_En_A(h.unpickle(a))

    private val en_A_to_En_S: En[A]=>En[S] = {
      case En(fid,id,prev,data) => En[S](fid,id,prev,f(data))
    }

    private val en_S_to_En_A: En[S]=>En[A] = {
      case En(fid,id,prev,data) => En[A](fid,id,prev,g(data))
    }
  }
}

/**
 * Abstract type entry handler
 * Since we don't know the exact type the pickler/unpickler still needs to be provided explicitly
 *
 * top --prev--> el --prev--> empty
 */
trait EnHandler[T] extends Handler[En[T]] {
  import Handler._
  val fh = implicitly[FdHandler]

  private def key(fid:String,id:String):String = s"${fid}.${id}"
  private def put(el:En[T])(implicit dba:Dba):Res[En[T]] = dba.put(key(el.fid,el.id),pickle(el)).right.map(_=>el)
  def get(fid:String,id:String)(implicit dba:Dba):Res[En[T]] = dba.get(key(fid,id)).right.map(unpickle)
  private def delete(fid:String,id:String)(implicit dba:Dba):Res[En[T]] = dba.delete(key(fid,id)).right.map(unpickle)

  /**
   * Adds the entry to the container
   * Creates the container if it's absent
   * @param el entry to add (prev is ignored). If id is empty it will be generated
   */
  def add(el: En[T])(implicit dba: Dba): Res[En[T]] =
    fh.get(Fd(el.fid)).left.map {
      _ => fh.put(Fd(el.fid)) // create feed if it doesn't exist
    }.joinLeft.right.map{ fd: Fd =>
      // id of entry must be unique
      get(el.fid,el.id).fold(
        l => {
          // generate ID if it is empty
          val id = if (el.id==empty) dba.nextid(el.fid) else el.id
          // add new entry with prev pointer
          put(el.copy(id=id,prev=fd.top)).right.map { added =>
            // update feed's top
            fh.put(fd.copy(top=id,count=fd.count+1)) match {
              case Right(_) => Right(added)
              case Left(err) => Left(err)
            }
          }.joinRight},
        r => Left(s"entry ${el.id} exist in ${el.fid}")
      )
    }.joinRight

  /**
   * Remove the entry from the container specified
   * @param el entry to remove (prev/data is ignored)
   * @return deleted entry (with data)
   */
  def remove(el: En[T])(implicit dba: Dba): Res[En[T]] =
    // get entry to delete
    get(el.fid,el.id).flatMap{ en =>
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
              .toRight("not found")
              .flatMap{ next =>
                // change link
                put(next.copy(prev=prev)).flatMap{ _ =>
                  // decrement count
                  fh.put(fd.copy(count=fd.count-1))
                }
              }
        ).flatMap(_ => delete(fid,id)) // delete entry
      }
    }

  def remove(fid:String,id:String)(implicit dba:Dba):Res[En[T]] =
    remove(new En[T](fid,id,prev=empty,data=null.asInstanceOf[T]))

  /** Iterates through container and return the list of entries.
   *
   * List is FILO ordered (most recent is first).
   * @param from if specified then return entries after this entry
   */
  def entries(fid:String,from:Option[En[T]])(implicit dba:Dba):Res[Vector[En[T]]] = {
    @tailrec def append(acc:Vector[En[T]],id:String):Vector[En[T]] =
      get(fid,id) match {
        case Right(en) => en.prev match {
          case `empty` => acc:+en
          case prev => append(acc:+en,prev)
        }
        case Left(err) =>
          log.error(s"Error while iterating=${err}")
          acc // return something
      }
    from match {
      case None => fh.get(Fd(fid)).map(_.top) match {
        case Left(x) => Left(x)
        case Right(`empty`) => Right(Vector.empty)
        case Right(start) => Right(append(acc=Vector.empty,id=start))
      }
      case Some(from) => from.prev match {
        case `empty` => Right(Vector.empty)
        case start => Right(append(acc=Vector.empty,id=start))
      }
    }
  }

  /** Iterates through container and return the stream of entries.
   *
   * Stream is FILO ordered (most recent is first).
   * @param from if specified then return entries after this entry
   */
  def stream(fid:String,from:Option[En[T]])(implicit dba:Dba):Res[Stream[En[T]]] = {
    def _stream(start:String):Stream[En[T]] =
      Stream.iterate(start=get(fid,start))(_.flatMap(x=>get(fid,x.prev)))
        .takeWhile(_.isRight)
        .flatMap(_.toOption)
    from match {
      case None => fh.get(Fd(fid)).map(_.top) match {
        case Left(x) => Left(x)
        case Right(`empty`) => Right(Stream.empty)
        case Right(start) => Right(_stream(start))
      }
      case Some(from) => from.prev match {
        case `empty` => Right(Stream.empty)
        case start => Right(_stream(start))
      }
    }
  }

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
}
