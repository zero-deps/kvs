package kvs
package store

import proto.*
import org.rocksdb.{util as _, *}
import zio.{ZIO, IO, Schedule, Has}
import zio.clock.Clock
import zio.duration.*

object Rks {
  case class Conf(dir: String = "data_rks")
  def apply(conf: Conf, clock: Clock.Service): Rks = new Rks(conf, clock)
}

class Rks(conf: Rks.Conf, clock: Clock.Service) extends Dba with AutoCloseable {
  RocksDB.loadLibrary()
  val dbopts = new Options()
    .setCreateIfMissing(true)
    .setCompressionType(CompressionType.LZ4_COMPRESSION)
  val db = RocksDB.open(dbopts, conf.dir)

  val env = Has(clock)

  implicit val elkeyc: MessageCodec[ElKey] = caseCodecAuto
  implicit val fdkeyc: MessageCodec[FdKey] = caseCodecAuto
  implicit val enkeyc: MessageCodec[EnKey] = caseCodecAuto
  implicit val pathc: MessageCodec[PathKey]  = caseCodecAuto
  implicit val chunkc: MessageCodec[ChunkKey] = caseCodecAuto
  implicit val keyc: MessageCodec[Key] = sealedTraitCodecAuto

  private def withRetryOnce[A](op: Array[Byte] => A, key: Key): ZIO[Clock, Err, A] =
    for {
      k <- IO.effectTotal(encode[Key](key))
      x <- IO.effect(op(k)).mapError(IOErr(_)).retry(Schedule.fromDuration(100 milliseconds))
    } yield x

  def get(key: Key): IO[Err, Option[Array[Byte]]] =
    for {
      x  <- withRetryOnce(db.get, key).provide(env)
      b  <- if (x == null) IO.succeed(None)
            else IO.effectTotal(Some(x))
    } yield b

  def put(key: Key, value: Array[Byte]): IO[Err, Unit] =
    withRetryOnce(db.put(_, value), key).provide(env)

  def del(key: Key): IO[Err, Unit] =
    withRetryOnce(db.delete, key).provide(env)

  def close(): Unit = {
    try{    db.close()}catch{case _:Throwable=>()}
    try{dbopts.close()}catch{case _:Throwable=>()}
  }
}
