package kvs
package store

import zero.ext._, option._
import proto._, macrosapi._
import org.rocksdb.{util=>_,_}
import zio.{ZIO, IO, Schedule, Has}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

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

  implicit val elkeyc = caseCodecAuto[ElKey]
  implicit val fdkeyc = caseCodecAuto[FdKey]
  implicit val enkeyc = caseCodecAuto[EnKey]
  implicit val pathc  = caseCodecAuto[PathKey]
  implicit val chunkc = caseCodecAuto[ChunkKey]
  implicit val keyc   = sealedTraitCodecAuto[Key]

  private def withRetryOnce[A](op: Array[Byte] => A, key: Key): ZIO[Clock, Err, A] =
    for {
      k <- IO.effectTotal(encode[Key](key))
      x <- IO.effect(op(k)).mapError(IOErr(_)).retry(Schedule.fromDuration(100 milliseconds))
    } yield x

  def get(key: Key): IO[Err, Option[Bytes]] =
    for {
      x  <- withRetryOnce(db.get, key).provide(env)
      b  <- if (x == null) IO.succeed(none)
            else IO.effectTotal(Bytes.unsafeWrap(x).some)
    } yield b

  def put(key: Key, value: Bytes): IO[Err, Unit] =
    withRetryOnce(db.put(_, value.unsafeArray), key).provide(env)

  def del(key: Key): IO[Err, Unit] =
    withRetryOnce(db.delete, key).provide(env)

  def close(): Unit = {
    try{    db.close()}catch{case _:Throwable=>()}
    try{dbopts.close()}catch{case _:Throwable=>()}
  }
}
