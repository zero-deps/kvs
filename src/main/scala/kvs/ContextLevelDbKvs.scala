package mws.kvs

import akka.serialization.Serialization
import org.iq80.leveldb.DB
import scala.reflect.ClassTag
import scala.reflect._

//abstract class ContextLevelDbKvs[T](val db: DB, val schemaName: String, val serialization: Serialization) extends ContextKvs[T] with LevelDbKvs[T]

trait ContextKvs[T] {//extends TypedKvs[T] {
//  override protected def clazz[T:ClassTag]: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
}