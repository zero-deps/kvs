package mws.kvs

/**
 * Container/Iterator types
 *
 * Case classed as functions and pickled take much less space when scala types.
 */
package object handle {
  final case class Fd(id:String,top:Option[String]=None,count:Int=0)
  final case class En[T](fid:String,id:String,prev:Option[String]=None,next:Option[String]=None,data:Option[T])

  object En {
    def apply[T](fid:String,id:String,data:T):En[T] = En(fid, id, prev=None, next=None, data=Some(data))
    def apply[T](fid:String,id:String):En[T] = En(fid, id, prev=None, next=None, data=None:Option[T])
  }
}
