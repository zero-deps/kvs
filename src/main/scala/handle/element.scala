package mws.kvs
package handle

import scalaz._, Scalaz._

import store._

trait ElHandler[T] extends Pickler[T] {
  def put(k:String,el:T)(implicit dba:Dba):Res[T] = dba.put(k,pickle(el)).map(_=>el)
  def get(k:String)(implicit dba:Dba):Res[T] = dba.get(k).flatMap(unpickle)
  def delete(k:String)(implicit dba:Dba):Res[T] = dba.delete(k).flatMap(unpickle)
}

object ElHandler {
  implicit object bytesHandler extends ElHandler[Array[Byte]]{
    def pickle(e:Array[Byte]):Array[Byte] = e
    def unpickle(a:Array[Byte]):Res[Array[Byte]] = a.right
  }
  implicit object strHandler extends ElHandler[String]{
    def pickle(e:String):Array[Byte] = e.getBytes("UTF-8")
    def unpickle(a:Array[Byte]):Res[String] = new String(a,"UTF-8").right
  }

  /**
   * Given handler S create the handler for T from conversion functions.
   */
  def by[A,S](f:A => S)(g:S => A)(key:String=>String)(implicit h:ElHandler[S]):ElHandler[A] = new ElHandler[A] {
    def pickle(e: A): Array[Byte] = h.pickle(f(e))
    def unpickle(a: Array[Byte]): Res[A] = h.unpickle(a).map(g)

    override def put(k:String,el:A)(implicit dba:Dba):Res[A] = h.put(key(k),f(el)).map(g)
    override def get(k:String)(implicit dba:Dba):Res[A] = h.get(key(k)).map(g)
    override def delete(k:String)(implicit dba:Dba):Res[A] = h.delete(key(k)).map(g)
  }
}
