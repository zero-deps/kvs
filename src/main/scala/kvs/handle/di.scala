package mws.kvs
package handle

import store._

/**
 * Di type handler.
 *
 * Basically Tuple2[String,String] for test purposes.
 */
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

  def add(el:D)(implicit dba:Dba) = put(el)
  def remove(el:D)(implicit dba:Dba) = delete(el._1)
  def entries(fid:String,from:Option[D],count:Option[Int])(implicit dba:Dba) = Right(Nil)
}
