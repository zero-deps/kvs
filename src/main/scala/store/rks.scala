package kvs
package store

import zero.ext._, option._
import proto._, macrosapi._
import org.rocksdb.{util=>_,_}
import zio._, duration._

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

  private def withRetryOnce[A](op: Array[Byte] => A, key: Key): KIO[A] =
    for {
      k <- IO.effectTotal(encode[Key](key))
      x <- IO.effect(op(k)).mapError(IOErr(_)).retry(Schedule.fromDuration(100 milliseconds))
    } yield x

  def get(key: Key): KIO[Option[Bytes]] =
    for {
      x  <- withRetryOnce(db.get, key)
      b  <- if (x == null) IO.succeed(none)
            else IO.effectTotal(Bytes.unsafeWrap(x).some)
    } yield b

  def put(key: Key, value: Bytes): KIO[Unit] =
    withRetryOnce(db.put(_, value.unsafeArray), key)

  def del(key: Key): KIO[Unit] =
    withRetryOnce(db.delete, key)

  def close(): Unit = {
    try{    db.close()}catch{case _:Throwable=>()}
    try{dbopts.close()}catch{case _:Throwable=>()}
  }
}
