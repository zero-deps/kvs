package zd.kvs
package el

import zd.kvs.store.Dba
import zero.ext._, either._, option._

trait ElHandler[T] {
  def pickle(e: T): Res[Array[Byte]]
  def unpickle(a: Array[Byte]): Res[T]

  def put(k:String,el:T)(implicit dba:Dba):Res[T] = pickle(el).flatMap(x => dba.put(k,x)).map(_=>el)
  def get(k:String)(implicit dba:Dba):Res[Option[T]] = dba.get(k) match {
    case Right(Some(x)) => unpickle(x).map(_.some)
    case Right(None) => Right(None)
    case x@Left(_) => x.coerceRight
  }
  def delete(k:String)(implicit dba:Dba):Res[Unit] = dba.delete(k)
}

object ElHandler {
  implicit object bytesHandler extends ElHandler[Array[Byte]]{
    def pickle(e:Array[Byte]):Res[Array[Byte]] = e.right
    def unpickle(a:Array[Byte]):Res[Array[Byte]] = a.right
  }
  implicit object strHandler extends ElHandler[String]{
    def pickle(e:String):Res[Array[Byte]] = e.getBytes("UTF-8").right
    def unpickle(a:Array[Byte]):Res[String] = new String(a,"UTF-8").right
  }
}
