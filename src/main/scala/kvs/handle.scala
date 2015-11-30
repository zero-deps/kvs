package mws.kvs

import store._
/**
 * KVS Handler for specific type T.
 * object Handler holds implicit default handlers.
 */
trait Handler[T]{
  def put(el:T)(implicit dba:Dba):Either[Err, T]
  def get(k:String)(implicit dba:Dba):Either[Err,T]
  def delete(k:String)(implicit dba:Dba):Either[Err,T]
}
object Handler{
  def apply[T](implicit h:Handler[T]) = h

  trait DHandler extends Handler[D]{
    def put(el:D)(implicit dba:Dba) = dba.put(el._1, el._2.getBytes) match {
      case Right(v) => Right((el._1, new String(v)))
      case Left(e) => Left(e)
    }

    def get(k:String)(implicit dba:Dba) = dba.get(k) match {
      case Right(v) => Right((k,new String(v)))
      case Left(e) => Left(e)
    }

    def delete(k:String)(implicit dba:Dba) = dba.delete(k) match {
      case Right(v) => Right((k,new String(v)))
      case Left(e)  => Left(e)
    }
  }

  implicit object dh extends DHandler
}
