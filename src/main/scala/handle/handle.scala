package mws.kvs
package handle

import scala.language.postfixOps
import store._

trait Pickler[T] {
  def pickle(e:T):Array[Byte]
  def unpickle(a:Array[Byte]):T
}

/**
 * KVS Handler for specific type T.
 * object Handler holds implicit default handlers.
 */
trait Handler[T] extends Pickler[T] {
  def add(el:T)(implicit dba:Dba):Res[T]
  def remove(el:T)(implicit dba:Dba):Res[T]
  def entries(fid:String,from:Option[T],count:Option[Int])(implicit dba:Dba):Res[List[T]]
}

object Handler {
  def apply[T](implicit h:Handler[T]) = h

  implicit object feedHandler extends FdHandler

  /**
   * The basic feed/entry handlers with scala-pickling serialization
   */
  implicit object strEnHandler extends EnHandler[String]{
    import scala.pickling._,Defaults._,binary._
    def pickle(e:En[String]) = e.pickle.value
    def unpickle(a:Array[Byte]) = a.unpickle[En[String]]
  }
  implicit object strTuple2EnHandler extends EnHandler[(String,String)]{
    import scala.pickling._,Defaults._,binary._
    def pickle(e:En[(String,String)]): Array[Byte] = e.pickle.value
    def unpickle(a: Array[Byte]): En[(String,String)] = a.unpickle[En[(String,String)]]
  }
  implicit object strTuple3Handler extends EnHandler[(String,String,String)]{
    import scala.pickling._,Defaults._,binary._
    def pickle(e:En[(String,String,String)]): Array[Byte] = e.pickle.value
    def unpickle(a: Array[Byte]): En[(String,String,String)] = a.unpickle[En[(String,String,String)]]
  }

  import scalaz._,Scalaz._

  /**
   * Given handler S create the handler for T from conversion functions.
   */
  def by[A,S](f:A => S)(g:S => A)(key:String=>String)(implicit h:Handler[S]):Handler[A] = new Handler[A] {
    def pickle(e: A): Array[Byte] = h.pickle(f(e))
    def unpickle(a: Array[Byte]): A = g(h.unpickle(a))

    def add(el:A)(implicit dba:Dba):Res[A] = h.add(f(el)).right.map(g)
    def remove(el:A)(implicit dba:Dba):Res[A] = h.remove(f(el)).right.map(g)
    def entries(fid:String,from:Option[A],count:Option[Int])(implicit dba:Dba):Res[List[A]] =
      h.entries(fid,toOpt(from),count).right.map { _ map g }

    private def toOpt = Functor[Option].lift(f)
  }
}
