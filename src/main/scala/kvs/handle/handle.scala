package mws.kvs
package handle

import store._
import scalaz._
import Scalaz._

/**
 * KVS Handler for specific type T.
 * object Handler holds implicit default handlers.
 */
trait Handler[T] {
  def put(el:T)(implicit dba:Dba):Either[Err, T]
  def get(k:String)(implicit dba:Dba):Either[Err,T]
  def delete(k:String)(implicit dba:Dba):Either[Err,T]
  def add(el:T)(implicit dba:Dba):Either[Err,T]
  def remove(el:T)(implicit dba:Dba):Either[Err,T]
  def entries(fid:String,from:Option[T],count:Option[Int])(implicit dba:Dba):Either[Err,List[T]]
  def entries(fid:String)(implicit dba:Dba):Either[Err,List[T]] = entries(fid,None,None)
}
object Handler{
  def apply[T](implicit h:Handler[T]) = h

  implicit object dh extends DHandler
  implicit object mh extends MessageHandler
  implicit object fdh extends FdHandler
  implicit object enh extends EHandler

  /**
   * Given handler S create the handler for T from conversion functions.
   */
  def by[T,S](f:T=>S)(g:S=>T)(ky:String=>String)(implicit h:Handler[S]):Handler[T] = new Handler[T] {
    def put(el:T)(implicit dba:Dba):Either[Err,T] = h.put(f(el)).right.map(g)
    def get(k:String)(implicit dba:Dba):Either[Err,T] = h.get(ky(k)).right.map(g)
    def delete(k:String)(implicit dba:Dba):Either[Err,T] = h.delete(ky(k)).right.map(g)
    def add(el:T)(implicit dba:Dba):Either[Err,T] = h.add(f(el)).right.map(g)
    def remove(el:T)(implicit dba:Dba):Either[Err,T] = h.remove(f(el)).right.map(g)
    def entries(fid:String,from:Option[T],count:Option[Int])(implicit dba:Dba):Either[Err,List[T]] = {
      def fo = Functor[Option].lift(f)
      h.entries(fid,fo(from),count).right.map { _ map g }
    }
    def by[T,S](f:T=>S)(g:S=>T) = this
  }

}
