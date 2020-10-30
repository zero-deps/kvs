package kvs.seq

import kvs.{Err => RootKvsErr}

sealed trait Err extends Throwable

abstract class ThrowableErr(error: Throwable) extends Err {
  override def getMessage() = s"`${this.getClass.getName()}` - `${error.getMessage()}`"
  override def getStackTrace() = error.getStackTrace()
  override def getCause() = error.getCause()
}

abstract class SimpleErr(msg: String = "") extends Err {
  val error: Throwable = new RuntimeException()
  override def getMessage() = s"`${this.getClass.getName()}` - `${msg}`"
  override def getStackTrace() = error.getStackTrace()
  override def getCause() = error.getCause()
}

case class DecodeErr(e: Throwable) extends ThrowableErr(e)
case class ShardErr(e: Throwable) extends ThrowableErr(e)
case class KvsErr(e: RootKvsErr) extends SimpleErr(e.toString)
