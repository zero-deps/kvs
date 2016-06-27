package mws.kvs

/**
 * Container/Iterator types
 */
package object handle {
  final case class Fd(id:String,top:Option[String])
  final case class En[T](fid:String,id:String,prev:Option[String],next:Option[String],data:T)

  object Fd {
    def apply(id:String):Fd = Fd(id,top=None)
  }

  object En {
    def apply[T](fid:String,id:String,data:T):En[T] = En(fid,id,prev=None,next=None,data)
    def apply[T](fid:String,id:String):En[T] = En(fid,id,prev=None,next=None,data=null.asInstanceOf[T])
  }
}
