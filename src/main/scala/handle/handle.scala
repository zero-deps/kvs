package mws.kvs
package handle

import store._

trait Pickler[T] {
  def pickle(e: T): Array[Byte]
  def unpickle(a: Array[Byte]): Res[T]
}

/**
 * KVS Handler for specific type T.
 * object Handler holds implicit default handlers.
 */
trait Handler[T] extends Pickler[T] {
  def add(el:T)(implicit dba:Dba):Res[T]
  def put(el:T)(implicit dba:Dba):Res[T]
  def remove(fid:String,id:String)(implicit dba:Dba):Res[T]
  def stream(fid: String, from: Option[T])(implicit dba: Dba): Res[Stream[Res[T]]]
  def get(fid:String,id:String)(implicit dba:Dba):Res[T]
}
