package mws.kvs

trait TypedKvs[T <: AnyRef] {
  def put(key: String, value: T): Either[Throwable, T]
  def get(key: String): Either[Throwable, Option[T]]
  def remove(key: String): Either[Throwable, Option[T]]

  protected def clazz: Class[T]
}
