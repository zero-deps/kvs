package mws.kvs

/**
 * Container/Iterator types
 *
 * Case classed as functions and pickled take much less space when scala types.
 */
package object handle {
  final case class Fd(id:String,top:Option[String],count:Int)
  final case class En[T](fid:String,id:String,prev:Option[String],next:Option[String],data:T)

  object Fd {
    def apply(id:String):Fd = Fd(id,top=None,count=0)
  }

  object En {
    def apply[T](fid:String,id:String,data:T):En[T] = En(fid,id,prev=None,next=None,data)
    def apply[T](fid:String,id:String):En[T] = En(fid,id,prev=None,next=None,data=null.asInstanceOf[T])
  }
}
