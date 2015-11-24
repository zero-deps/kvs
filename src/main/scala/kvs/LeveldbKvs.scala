package mws.kvs

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import java.io.File
import com.typesafe.config.Config
import org.fusesource.leveldbjni.JniDBFactory._
import org.iq80.leveldb._
import scala.language.implicitConversions
import scala.util.Try
import LeveldbKvs._

object LeveldbKvs {
  implicit def toBytes(value: String): Array[Byte] = bytes(value)
  implicit def fromBytes(value: Array[Byte]): String = asString(value)

  def apply(config: Config)(implicit system: ActorSystem): StKvs =
    new LeveldbKvs(config, system.log)
}

class LeveldbKvs(config: Config, log: LoggingAdapter) extends StKvs {
  val leveldbOptions = new Options().createIfMissing(true)
  def leveldbReadOptions = new ReadOptions().verifyChecksums(config.checksum)
  val leveldbWriteOptions = new WriteOptions().sync(config.fsync).snapshot(false)
  val leveldb: DB = leveldbFactory.open(config.dir,
    if (config.native) leveldbOptions
    else leveldbOptions.compressionType(CompressionType.NONE))

  def leveldbFactory =
    if (config.native) org.fusesource.leveldbjni.JniDBFactory.factory
    else org.iq80.leveldb.impl.Iq80DBFactory.factory

  def put(key: String, value: String): Unit = leveldb.put(key, value)

  def get(key: String): Option[String] = Option(leveldb.get(key))

  def delete(key: String): Unit = leveldb.delete(key)

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
}
