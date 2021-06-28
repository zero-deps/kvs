package zd.kvs

import proto.*
import org.rocksdb.*
import zio.*, clock.*, duration.*
import scala.util.Try
import scala.util.chaining.*
import scala.concurrent.Future

class Rks(dir: String) extends Dba, AutoCloseable:
  RocksDB.loadLibrary()
  private val opts = Options().nn
    .setCreateIfMissing(true).nn
    .setCompressionType(CompressionType.LZ4_COMPRESSION).nn
  private val db = RocksDB.open(opts, dir).nn

  private def withRetryOnce[A](op: Array[Byte] => A, key: K): R[A] =
    val eff = for {
      k <- IO.effectTotal(zd.rng.stob(key))
      x <- IO.effect(op(k)).retry(Schedule.fromDuration(100 milliseconds))
    } yield x
    Try(Runtime.default.unsafeRun(eff.either)).toEither.flatten.leftMap(Failed)

  override def get(key: K): R[Option[V]] =
    for {
      x  <- withRetryOnce(db.get, key)
    } yield if x != null then Some(x) else None

  override def put(key: K, value: V): R[Unit] =
    withRetryOnce(db.put(_, value), key)

  override def delete(key: K): R[Unit] =
    withRetryOnce(db.delete, key)

  def compact(): Unit = db.compactRange()
  def nextid(fid: String): R[String] = ??? //todo

  def save(path: String): R[String] = ??? //todo
  def load(path: String): R[String] = ??? //todo

  def onReady(): Future[Unit] = Future.successful(unit)
  def deleteByKeyPrefix(keyPrefix: K): R[Unit] = ???

  override def close(): Unit =
    try { db.close() } catch { case _: Throwable => unit }
    try { opts.close() } catch { case _: Throwable => unit }

  given [A]: CanEqual[A, A | Null] = CanEqual.derived
