package mws.kvs

import akka.serialization.Serialization
import org.fusesource.leveldbjni.JniDBFactory
import org.iq80.leveldb.DB

trait LevelDbKvs[T <: AnyRef] extends TypedKvs[T] {

  def serialization: Serialization
  def schemaName: String
  def db: DB

  override def put(key: String, value: T): Either[Throwable, T] = {
    try {
      db.put(JniDBFactory.bytes(composeKey(key)), serialization.serialize(value).get)
      Right(value)
    } catch {
      case t: Throwable => Left(t)
    }
  }

  override def get(key: String): Either[Throwable, Option[T]] = {
    try {
      Right(db.get(JniDBFactory.bytes(composeKey(key))) match {
        case bytes: Array[Byte] => Some(serialization.deserialize(bytes, clazz).get)
        case null => None
      })
    } catch {
      case t: Throwable => Left(t)
    }
  }

  override def remove(key: String): Either[Throwable, Option[T]] = {
    get(key) match {
      case Left(ex) => Left(ex)
      case Right(opt) =>
        opt match {
          case Some(value) if value != null && value.isInstanceOf[T] =>
            try {
              db.delete(JniDBFactory.bytes(composeKey(key)))
              Right(Some(value))
            } catch {
              case t: Throwable => Left(t)
            }
          case None => Right(None)
        }
    }
  }

  protected def composeKey(k: String): String = (schemaName, k).toString()
}