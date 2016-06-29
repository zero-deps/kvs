package mws.kvs
package handle

import store._

trait ElHandler[T] extends Pickler[T] {
  def put(k:String,el:T)(implicit dba:Dba):Either[Err,T] = dba.put(k,pickle(el)).right.map(_=>el)
  def get(k:String)(implicit dba:Dba):Either[Err,T] = dba.get(k).right.map(unpickle)
  def delete(k:String)(implicit dba:Dba):Either[Err,T] = dba.delete(k).right.map(unpickle)
}

object ElHandler {
  implicit object strHandler extends ElHandler[String]{
    def pickle(e:String):Array[Byte] = e.getBytes("UTF-8")
    def unpickle(a:Array[Byte]):String = new String(a,"UTF-8")
  }

  /**
   * Given handler S create the handler for T from conversion functions.
   */
  def by[A,S](f:A => S)(g:S => A)(key:String=>String)(implicit h:ElHandler[S]):ElHandler[A] = new ElHandler[A] {
    def pickle(e: A): Array[Byte] = h.pickle(f(e))
    def unpickle(a: Array[Byte]): A = g(h.unpickle(a))

    override def put(k:String,el:A)(implicit dba:Dba):Res[A] = h.put(key(k),f(el)).right.map(g)
    override def get(k:String)(implicit dba:Dba):Res[A] = h.get(key(k)).right.map(g)
    override def delete(k:String)(implicit dba:Dba):Res[A] = h.delete(key(k)).right.map(g)
  }
}