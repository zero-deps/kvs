package kvs
package store

import concurrent.Future
import zero.ext._, option._, either._
import zd.proto._, api._, macrosapi._
import org.rocksdb.{util=>_,_}

object Rks {
  case class Conf(dir: String = "data_rks")
  def apply(conf: Conf): Rks = new Rks(conf)
}

class Rks(conf: Rks.Conf) extends Dba with AutoCloseable {
  RocksDB.loadLibrary()
  val dbopts = new Options()
    .setCreateIfMissing(true)
    .setCompressionType(CompressionType.LZ4_COMPRESSION)
  val db = RocksDB.open(dbopts, conf.dir)

  implicit val elkeyc = caseCodecAuto[ElKey]
  implicit val fdkeyc = caseCodecAuto[FdKey]
  implicit val enkeyc = caseCodecAuto[EnKey]
  implicit val pathc  = caseCodecAuto[PathKey]
  implicit val chunkc = caseCodecAuto[ChunkKey]
  implicit val keyc   = sealedTraitCodecAuto[Key]

  def get(key: Key): Res[Option[Bytes]] = {
    fromNullable(Bytes.unsafeWrap(db.get(encode[Key](key)))).right
  }

  def put(key: Key, value: Bytes): Res[Unit] = {
    db.put(encode[Key](key), value.unsafeArray).right
  }

  def delete(key: Key): Res[Unit] = {
    db.delete(encode[Key](key)).right
  }

  def load(path: String): Res[Any] = ???
  def save(path: String): Res[String] = ???

  def onReady(): Future[Unit] = Future.successful(())
  def compact(): Unit = db.compactRange()
  def close(): Unit = {
    db.close()
    dbopts.close()
  }
}
