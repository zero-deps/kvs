package mws.kvs
package handle

import mws.kvs.store._

trait FdHandler extends Pickler[Fd] {
  def put(el:Fd)(implicit dba:Dba):Res[Fd] = dba.put(el.id,pickle(el)).flatMap(unpickle)
  def get(el:Fd)(implicit dba:Dba):Res[Fd] = dba.get(el.id).flatMap(unpickle)
  def delete(el:Fd)(implicit dba:Dba):Res[Fd] = dba.delete(el.id).flatMap(unpickle)
}
