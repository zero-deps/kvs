package mws.kvs
package handle

import scala.util.Try

import scalaz._, Scalaz._

import store._

trait FdHandler extends Pickler[Fd] {
  def put(el:Fd)(implicit dba:Dba):Res[Fd] = dba.put(el.id,pickle(el)).flatMap(unpickle)
  def get(el:Fd)(implicit dba:Dba):Res[Fd] = dba.get(el.id).flatMap(unpickle)
  def delete(el:Fd)(implicit dba:Dba):Res[Fd] = dba.delete(el.id).flatMap(unpickle)
}
