package mws.kvs
package handle

import store._
import scala.language.implicitConversions
import scala.language.postfixOps

object EnHandler {
  /**
   * Given EnHandler S create the EnHandler for A from conversion functions.
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
 * Abstract type entry handler.
 *
 * Since we don't know the exact type the pickler/unpickler still needs to be provided explicitly.
 */
trait EnHandler[T] extends Handler[En[T]] {
  import Handler._
  val fh = implicitly[Handler[Fd]]

  private implicit def tuple2ToId(fid:(String,String)):String = s"${fid._1}.${fid._2}"

  def put(el: En[T])(implicit dba: Dba): Res[En[T]] = dba.put((el.fid, el.id), pickle(el)).right.map { _ => el }
  def get(k: String)(implicit dba: Dba): Res[En[T]] = dba.get(k).right.map(unpickle)
  final def get(fid: String, id: String)(implicit dba: Dba): Res[En[T]] = get((fid, id))
  def delete(k: String)(implicit dba: Dba): Res[En[T]] = dba.delete(k).right.map(unpickle)
  final def delete(fid: String, id: String)(implicit dba: Dba): Res[En[T]] = delete((fid, id))

  /**
   * Adds the entry to the container specified as id.
   * Creates the container with specified id if its absent.
   *
   * todo: merge top/el update error cases. top update failures now skipped
   */
  def add(el: En[T])(implicit dba: Dba): Res[En[T]] = {
    fh.get(el.fid).left.map {
      case Dbe("error", _) => fh.put(new Fd(el.fid, None, 0))
    }.joinLeft.right.map { feed: Fd =>
      get(el.fid, el.id).fold(_ =>
        put(el.copy(prev = feed.top)).right.map { _ =>
          //update top
          feed.top.map { id => get(el.fid, id).right.map { a => put(a.copy(next = Some(el.id))) }.joinRight }
          //update el
          fh.put(feed.copy(top = Some(el.id), count = feed.count + 1)).
            fold(l => Left(l), r => Right(el.copy(prev = feed.top)))
        }.joinRight.swap,
        _ => Right(Dbe("error", s"entry ${el.id} exist in ${el.fid}"))).swap
    }.joinRight
  }

  /**
   * Remove the entry from the container specified as id.
   * todo: check failure cases
    * next is ignored
   */
  def remove(el: En[T])(implicit dba: Dba): Res[En[T]] = {
    delete(el.fid, el.id) match {
      case Left(l) => Left(l)
      case Right(r) => r match {
        //case top element of feed is removed
        case En(fid, _, prev, None, _) =>
          prev map { x => get((fid,x)).right.map { n => put(n.copy(next = None)) }}
          fh.get(fid).right.map { feed =>
              fh.put(feed.copy(top = prev,count = feed.count - 1))
          }.joinRight.right.map { _ => r }
        //other cases
        case En(fid, _, prev, Some(next), _) =>
           prev map { x => get((fid,x)).right.map { p => put(p.copy(next = Some(next))) }}
           get((fid,next)).right.map { n => put(n.copy(prev = prev)) }
          fh.get(fid).right.map {
            (feed: Fd) =>
            fh.put(feed.copy(count = feed.count - 1))
          }.joinRight.right.map { _ => r }
      }}
  }

  /**
   * Iterate through container and return the list of entry with specified size.
   */
  import scalaz._, Scalaz._

  def entries(fid: String, from: Option[En[T]], count: Option[Int])(implicit dba: Dba): Res[List[En[T]]] = fh.get(fid).fold(
    l => Left(Dbe("error", s"$fid $l")),
    r => r match {
      case Fd(`fid`, top, size) =>
        val none: Res[En[T]] = Left(Dbe(msg = "done"))
        def next: (Res[En[T]]) => Res[En[T]] = _.right.map { _.prev.fold(none)({ id => get(fid, id) }) }.joinRight

        (from map { _.id } orElse top).map { eid => (fid, eid) }.map { //determine start entry
          start =>
          List.iterate(get(start), count map { x => x min size } getOrElse size)(next).sequenceU //iterate through list
        }.getOrElse(Right(Nil))
    })
}
