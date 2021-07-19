package zd.kvs

import akka.actor.*
import akka.event.Logging
import akka.pattern.ask
import akka.routing.FromConfig
import akka.util.{Timeout}
import leveldbjnr.LevelDb
import zd.rng
import zd.rng.store.{ReadonlyStore, WriteStore}
import zd.rng.stob
import scala.concurrent.*
import scala.concurrent.duration.*
import scala.util.{Try, Success, Failure}

class Rng(system: ActorSystem) extends Dba, AutoCloseable:
  private val log = Logging(system, "hash-ring")

  private val cfg = system.settings.config.getConfig("ring").nn

  system.eventStream

  private val leveldbPath = cfg.getString("leveldb.dir").nn
  private val db: LevelDb = LevelDb.open(leveldbPath).fold(l => throw l, r => r)

  system.actorOf(WriteStore.props(db).withDeploy(Deploy.local), name="ring_write_store")
  system.actorOf(FromConfig.props(ReadonlyStore.props(db)).withDeploy(Deploy.local), name="ring_readonly_store")

  private val hash = system.actorOf(rng.Hash.props(db).withDeploy(Deploy.local), name="ring_hash")

  override def put(key: K, value: V): R[Unit] =
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").nn.toNanos)
    val t = Timeout(d)
    val putF = hash.ask(rng.Put(stob(key), value))(t).mapTo[rng.Ack]
    Try(Await.result(putF, d)) match
      case Success(rng.AckSuccess(_)) => Right(())
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(rng.AckTimeoutFailed(op, k)) => Left(RngAskTimeoutFailed(op, k))
      case Failure(t) => Left(Failed(t))

  private def isReady(): Future[Boolean] =
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").nn.toNanos)
    val t = Timeout(d)
    hash.ask(rng.Ready)(t).mapTo[Boolean]

  override def onReady(): Future[Unit] =
    val p = Promise[Unit]()
    def loop(): Unit =
      import system.dispatcher
      system.scheduler.scheduleOnce(1 second){
        isReady() onComplete {
          case Success(true) =>
            log.info("KVS is ready")
            p.success(())
          case _ =>
            log.info("KVS isn't ready yet...")
            loop()
        }
      }
    loop()
    p.future

  override def get(key: K): R[Option[V]] =
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").nn.toNanos)
    val t = Timeout(d)
    val fut = hash.ask(rng.Get(stob(key)))(t).mapTo[rng.Ack]
    Try(Await.result(fut, d)) match
      case Success(rng.AckSuccess(v)) => Right(v)
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(rng.AckTimeoutFailed(op, k)) => Left(RngAskTimeoutFailed(op, k))
      case Failure(t) => Left(Failed(t))

  override def delete(key: K): R[Unit] =
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").nn.toNanos)
    val t = Timeout(d)
    val fut = hash.ask(rng.Delete(stob(key)))(t).mapTo[rng.Ack]
    Try(Await.result(fut, d)) match
      case Success(rng.AckSuccess(_)) => Right(())
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(rng.AckTimeoutFailed(op, k)) => Left(RngAskTimeoutFailed(op, k))
      case Failure(t) => Left(Failed(t))

  override def save(path: String): R[String] =
    val d = 1 hour
    val x = hash.ask(rng.Save(path))(Timeout(d))
    Try(Await.result(x, d)) match
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(v: String) => Right(v)
      case Success(v) => Left(RngFail(s"Unexpected response: ${v}"))
      case Failure(t) => Left(Failed(t))

  def iterate(f: (K, V) => Unit): R[String] =
    val d = 1 hour
    val x = hash.ask(rng.Iterate((key, value) => f(new String(key, "UTF-8"), value)))(Timeout(d))
    Try(Await.result(x, d)) match
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(v: String) => Right(v)
      case Success(v) => Left(RngFail(s"Unexpected response: ${v}"))
      case Failure(t) => Left(Failed(t))

  override def load(path: String): R[String] =
    val d = Duration.fromNanos(cfg.getDuration("dump-timeout").nn.toNanos)
    val t = Timeout(d)
    val x = hash.ask(rng.Load(path))(t)
    Try(Await.result(x, d)) match
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(v: String) => Right(v)
      case Success(v) => Left(RngFail(s"Unexpected response: ${v}"))
      case Failure(t) => Left(Failed(t))

  override def compact(): Unit =
    db.compact()

  override def deleteByKeyPrefix(k: K): R[Unit] =
    val d = Duration.fromNanos(cfg.getDuration("iter-timeout").nn.toNanos)
    val t = Timeout(d)
    val x = hash.ask(rng.Iter(stob(k)))(t)
    Try(Await.result(x, d)) match
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(res: zd.rng.IterRes) =>
        res.keys.foreach(log.info)
        res.keys.map(delete).sequence_
      case Success(v) => Left(RngFail(s"Unexpected response: ${v}"))
      case Failure(t) => Left(Failed(t))

  override def close(): Unit =
    try { db.close() } catch { case _: Throwable => () }
end Rng
