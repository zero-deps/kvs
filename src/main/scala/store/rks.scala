package kvs
package store

import zero.ext._, option._
import zd.proto._, api._, macrosapi._
import org.rocksdb.{util=>_,_}
import zio._

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

  def get(key: Key): KUIO[Option[Bytes]] = {
    IO.succeed(fromNullable(Bytes.unsafeWrap(db.get(encode[Key](key)))))
  }

  def put(key: Key, value: Bytes): KUIO[Unit] = {
    IO.succeed(db.put(encode[Key](key), value.unsafeArray))
  }

  def del(key: Key): KUIO[Unit] = {
    IO.succeed(db.delete(encode[Key](key)))
  }

  def close(): Unit = {
    db.close()
    dbopts.close()
  }
}
