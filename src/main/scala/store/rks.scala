package zd.kvs

import akka.actor.*
import akka.event.Logging
import akka.pattern.ask
import akka.util.{Timeout}
import akka.event.LoggingAdapter
import proto.*
import org.rocksdb.*
import zio.*, clock.*, duration.*
import scala.concurrent.*
import scala.concurrent.duration.*
import scala.util.{Try, Success, Failure}
import scala.util.chaining.*
import scala.concurrent.Future
import zd.rks.DumpProcessor

class Rks(system: ActorSystem, dir: String) extends Dba, AutoCloseable:
  RocksDB.loadLibrary()
  private val logging: LoggingAdapter = Logging(system, "rks")
  private val cfg = system.settings.config.getConfig("rks").nn
  private val opts = Options().nn
    .setCreateIfMissing(true).nn
    .setCompressionType(CompressionType.LZ4_COMPRESSION).nn
    .setLogger(new Logger(Options()) {
      def log(infoLogLevel: InfoLogLevel, logMsg: String): Unit =
        infoLogLevel match
          case InfoLogLevel.DEBUG_LEVEL => logging.debug(logMsg)
          case InfoLogLevel.INFO_LEVEL => logging.debug(logMsg)
          case InfoLogLevel.WARN_LEVEL => logging.warning(logMsg)
          case _ => logging.error(logMsg)
    }).nn
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

  def save(path: String): R[String] =
    val d = FiniteDuration(1, HOURS)
    val dump = system.actorOf(DumpProcessor.props(db), s"dump_wrkr-${System.currentTimeMillis}")
    val x = dump.ask(DumpProcessor.Save(path))(Timeout(d))
    Try(Await.result(x, d)) match
      case Success(v: String) => Right(v)
      case Success(v) => Left(RngFail(s"Unexpected response: ${v}"))
      case Failure(t) => Left(Failed(t))

  def load(path: String): R[String] =
    val d = concurrent.duration.Duration.fromNanos(cfg.getDuration("dump-timeout").nn.toNanos)
    val dump = system.actorOf(DumpProcessor.props(db), s"dump_wrkr-${System.currentTimeMillis}")
    val x = dump.ask(DumpProcessor.Load(path))(Timeout(d))
    Try(Await.result(x, d)) match
      case Success(v: String) => Right(v)
      case Success(v) => Left(RngFail(s"Unexpected response: ${v}"))
      case Failure(t) => Left(Failed(t))

  def onReady(): Future[Unit] = Future.successful(unit)
  def deleteByKeyPrefix(keyPrefix: K): R[Unit] = ???

  override def close(): Unit =
    try { db.close() } catch { case _: Throwable => unit }
    try { opts.close() } catch { case _: Throwable => unit }

  given [A]: CanEqual[A, A | Null] = CanEqual.derived
