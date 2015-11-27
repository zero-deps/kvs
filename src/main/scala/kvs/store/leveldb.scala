package mws.kvs
package store

import akka.actor.ActorSystem
import akka.serialization._
import akka.event.LoggingAdapter
import java.io.File
import com.typesafe.config.Config
import org.fusesource.leveldbjni.JniDBFactory
import org.fusesource.leveldbjni.JniDBFactory._
import org.iq80.leveldb._
import scala.language.implicitConversions
import scala.util.Try
import akka.serialization.Serialization
import scala.concurrent.Future
import mws.rng._

object Leveldb {
  implicit def toBytes(value: String): Array[Byte] = bytes(value)
  implicit def fromBytes(value: Array[Byte]): String = asString(value)

  def apply()(implicit system: ActorSystem): Kvs = new Leveldb(system)
}

class Leveldb(system: ActorSystem) extends Kvs {
  import Leveldb._
  import scala.language.implicitConversions
  val config = system.settings.config.getConfig("leveldb")
  val log = system.log
  val serialization = SerializationExtension(system)

  val leveldbOptions = new Options().createIfMissing(true)
  def leveldbReadOptions = new ReadOptions().verifyChecksums(config.checksum)
  val leveldbWriteOptions = new WriteOptions().sync(config.fsync).snapshot(false)
  val leveldb: DB = leveldbFactory.open(config.dir,
    if (config.native) leveldbOptions
    else leveldbOptions.compressionType(CompressionType.NONE))

  def leveldbFactory =
    if (config.native) org.fusesource.leveldbjni.JniDBFactory.factory
    else org.iq80.leveldb.impl.Iq80DBFactory.factory

//  def put(key: String,str: AnyRef): Future[mws.rng.Ack] = ???
  def put(key: String, value: AnyRef): Future[Ack] = {
    leveldb.put(key, serialization.serialize(value).get)
    Future.successful(AckSuccess)
  }

  def get[T](key: String, clazz:Class[T]): Future[Option[T]] = {
    Future.successful(Some(serialization.deserialize(leveldb.get(key), clazz).get))
  }

  def delete(key: String) = {
    leveldb.delete(key)
    Future.successful(AckSuccess)
  }

  def close(): Unit = Try(leveldb.close())

  implicit class LeveldbConfig(config: Config) {
    def native: Boolean = sys.props.get("os.name") match {
      case Some(os) if os.startsWith("Windows") =>
        log.info("Windows has been detected. Forcing usage of Java port of LevelDB")
        false
      case _ => config.getBoolean("native")
    }
    def checksum: Boolean = config.getBoolean("checksum")
    def fsync: Boolean = config.getBoolean("fsync")
    def dir: File = new File(config.getString("dir"))
  }

  def isReady = Future.successful(true)

  def entries: Iterator[String] = Iterator.empty
}

//

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