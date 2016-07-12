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
      case En(fid,id,prev,next,data) => En[S](fid,id,prev,next,f(data))
    }

    private val en_S_to_En_A: En[S]=>En[A] = {
      case En(fid,id,prev,next,data) => En[A](fid,id,prev,next,g(data))
    }
  }
}

/**
 * Abstract type entry handler
 * Since we don't know the exact type the pickler/unpickler still needs to be provided explicitly
 *
 *               top -prev-> el -prev-> empty
 * empty <-next- top <-next- el
 */
trait EnHandler[T] extends Handler[En[T]] {
  import Handler._
  val fh = implicitly[FdHandler]

  def key(fid:String,id:String):String = s"$fid.$id"
  def put(el:En[T])(implicit dba:Dba):Res[En[T]] = dba.put(key(el.fid,el.id),pickle(el)).right.map(_=>el)
  def get(fid:String,id:String)(implicit dba:Dba):Res[En[T]] = dba.get(key(fid,id)).right.map(unpickle)
  def delete(fid:String,id:String)(implicit dba:Dba):Res[En[T]] = dba.delete(key(fid,id)).right.map(unpickle)

  /**
   * Adds the entry to the container
   * Creates the container if it's absent
   * @param el entry to add (prev/next is ignored)
   */
  def add(el: En[T])(implicit dba: Dba): Res[En[T]] = {
    fh.get(Fd(el.fid)).left.map {
      _ => fh.put(Fd(el.fid)) // create feed if it doesn't exist
    }.joinLeft.right.map{ fd: Fd =>
      // id of entry must be unique
      get(el.fid,el.id).fold(
        l =>
          // add new entry with prev pointer
          put(el.copy(prev=fd.top,next=empty)).right.map { added =>
            fd.top match {
              case `empty` =>
                // feed is empty
                // update feed's top
                fh.put(fd.copy(top=el.id)) match {
                  case Right(_) => Right(added)
                  case Left(err) => Left(err)
                }
              case old_top =>
                // set next pointer for old top
                get(el.fid, old_top).right.map(old_top => put(old_top.copy(next=el.id)).right.map{_ =>
                  // update feed's top
                  fh.put(fd.copy(top=el.id)) match {
                    case Right(_) => Right(added)
                    case Left(err) => Left(err)
                  }
                }.joinRight).joinRight
            }
          }.joinRight,
        r => Left(s"entry ${el.id} exist in ${el.fid}")
      )
    }.joinRight
  }

  /**
   * Remove the entry from the container specified
   * @param el entry to remove (prev/next/data is ignored)
   * @return deleted entry (with data)
   */
  def remove(el: En[T])(implicit dba: Dba): Res[En[T]] = {
    def `change next pointer of 'prev'`(el:En[T]):Res[En[T]] = el.prev match {
      case `empty` =>
        `change prev pointer of 'next'`(el)
      case prev =>
        get(el.fid,prev).right.map(prev=>put(prev.copy(next=el.next)).right.map{_ =>
          `change prev pointer of 'next'`(el)
        }.joinRight).joinRight
    }
    def `change prev pointer of 'next'`(el:En[T]):Res[En[T]] = el.next match {
      case `empty` =>
        `change top`(el)
      case next =>
        get(el.fid,next).right.map(next=>put(next.copy(prev=el.prev)).right.map{_ =>
          `delete entry`(el)
        }.joinRight).joinRight
    }
    def `change top`(el:En[T]):Res[En[T]] =
      fh.get(Fd(el.fid)).right.map(fd=>fh.put(fd.copy(top=el.prev)).right.map{_ =>
        `delete entry`(el)
      }.joinRight).joinRight
    def `delete entry`(el:En[T]):Res[En[T]] = delete(el.fid,el.id)
    // entry must exist
    get(el.fid,el.id).right.map(`change next pointer of 'prev'`).joinRight
  }

  /**
   * Iterate through container and return the list of entry with specified size.
   * List is inserted ordered (first added is first in list).
   * @param from if specified then return entries after this entry
   */
  def entries(fid:String,from:Option[En[T]],count:Option[Int])(implicit dba:Dba):Res[List[En[T]]] =
    fh.get(Fd(fid)).right.map{ fd =>
      def prev_res: (Res[En[T]]) => Res[En[T]] = _.right.map(x=>get(fid,x.prev)).joinRight
      val start:String = from match {
        case None => fd.top
        case Some(from) => from.prev
      }
      @tailrec def iterate(acc:List[En[T]],id:String):Res[List[En[T]]] = count match {
        case Some(count) if count == acc.length => Right(acc) // limit results
        case _ =>
          get(fid,id) match {
            case Right(en) => en.prev match {
              case `empty` => Right(en::acc)
              case prev => iterate(en::acc,prev)
            }
            case Left(err) => Left(err) // in case of error discard acc
          }
        }
      if (start == empty) Right(Nil)
      else iterate(acc=Nil,id=start)
    }.joinRight
}
