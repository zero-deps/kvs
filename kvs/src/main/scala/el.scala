package zd.kvs

trait ElHandler[T]:
  def pickle(e: T): Array[Byte]
  def unpickle(a: Array[Byte]): T

  def put(k: String, el: T)(using dba: Dba): Either[Err, T] =
    dba.put(k, pickle(el)).map(_ => el)

  def get(k: String)(using dba: Dba): Either[Err, Option[T]] =
    dba.get(k).map(_.map(unpickle))
  
  def delete(k: String)(using dba: Dba): Either[Err, Unit] =
    dba.delete(k).void

object ElHandler:
  given ElHandler[Array[Byte]] = new ElHandler:
    def pickle(e: Array[Byte]): Array[Byte] = e
    def unpickle(a: Array[Byte]): Array[Byte] = a

  given ElHandler[String] = new ElHandler:
    def pickle(e: String): Array[Byte] = e.getBytes("utf8").nn
    def unpickle(a: Array[Byte]): String = String(a, "utf8").nn
