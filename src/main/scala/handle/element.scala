package mws.kvs
package handle

import store._

trait ElHandler[T] extends Handler[T] {

  def put(el:T)(implicit dba:Dba):Res[T] = ???
  def get(k:String)(implicit dba:Dba):Res[T] = ???
  def delete(k:String)(implicit dba:Dba):Res[T] = ???

  def add(el:T)(implicit dba:Dba):Res[T] = ???
  def remove(el:T)(implicit dba:Dba):Res[T] = ???
  def entries(fid:String,from:Option[T],count:Option[Int])(implicit dba:Dba):Res[List[T]] = ???
}