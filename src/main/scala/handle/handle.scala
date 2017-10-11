package mws.kvs
package handle

import scalaz._

import store._

trait Pickler[T] {
  def pickle(e:T):Array[Byte]
  def unpickle(a:Array[Byte]):Res[T]
}

/**
 * KVS Handler for specific type T.
 * object Handler holds implicit default handlers.
 */
trait Handler[T] extends Pickler[T] {
  def add(el:T)(implicit dba:Dba):Res[T]
  def remove(fid:String,id:String)(implicit dba:Dba):Res[T]
  def stream(fid:String,from:Maybe[T])(implicit dba:Dba):Res[Stream[T]]
  def get(fid:String,id:String)(implicit dba:Dba):Res[T]
}

object Handler {
  /**
   * Given handler S create the handler for T from conversion functions.
   */
  def by[A,S](f:A => S)(g:S => A)(key:String=>String)(implicit h:Handler[S]):Handler[A] = new Handler[A] {
    def pickle(e: A): Array[Byte] = h.pickle(f(e))
    def unpickle(a: Array[Byte]): Res[A] = h.unpickle(a).map(g)

    def add(el:A)(implicit dba:Dba):Res[A] = h.add(f(el)).map(g)
    def remove(fid:String,id:String)(implicit dba:Dba):Res[A] = h.remove(fid,id).map(g)
    def stream(fid:String,from:Maybe[A])(implicit dba:Dba):Res[Stream[A]] =
      h.stream(fid,toOpt(from)).map { _ map g }
    def get(fid:String,id:String)(implicit dba:Dba):Res[A] = h.get(fid,id).map(g)

    private def toOpt = Functor[Maybe].lift(f)
  }
}
