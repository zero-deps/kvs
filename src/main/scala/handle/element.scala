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
}
