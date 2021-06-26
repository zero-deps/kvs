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

class Rng(system: ActorSystem) extends Dba {
  lazy val log = Logging(system, "hash-ring")

  val cfg = system.settings.config.getConfig("ring").nn

  system.eventStream

  val leveldbPath = cfg.getString("leveldb.dir").nn
  val leveldb: LevelDb = LevelDb.open(leveldbPath).fold(l => throw l, r => r)

  system.actorOf(WriteStore.props(leveldb).withDeploy(Deploy.local), name="ring_write_store")
  system.actorOf(FromConfig.props(ReadonlyStore.props(leveldb)).withDeploy(Deploy.local), name="ring_readonly_store")

  val hash = system.actorOf(rng.Hash.props(leveldb).withDeploy(Deploy.local), name="ring_hash")

  def put(key: String, value: V): Either[Err, V] = {
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").nn.toNanos)
    val t = Timeout(d)
    val putF = hash.ask(rng.Put(stob(key), value))(t).mapTo[rng.Ack]
    Try(Await.result(putF, d)) match {
      case Success(rng.AckSuccess(_)) => Right(value)
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(rng.AckTimeoutFailed(op, k)) => Left(RngAskTimeoutFailed(op, k))
      case Failure(t) => Left(RngThrow(t))
    }
  }

  private def isReady(): Future[Boolean] = {
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").nn.toNanos)
    val t = Timeout(d)
    hash.ask(rng.Ready)(t).mapTo[Boolean]
  }

  def onReady(): Future[Unit] = {
    val p = Promise[Unit]()
    def loop(): Unit = {
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
      ()
    }
    loop()
    p.future
  }

  def get(key: String): Either[Err, Option[V]] = {
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").nn.toNanos)
    val t = Timeout(d)
    val fut = hash.ask(rng.Get(stob(key)))(t).mapTo[rng.Ack]
    Try(Await.result(fut, d)) match {
      case Success(rng.AckSuccess(v)) => Right(v)
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(rng.AckTimeoutFailed(op, k)) => Left(RngAskTimeoutFailed(op, k))
      case Failure(t) => Left(RngThrow(t))
    }
  }

  def delete(key: String): Either[Err, Unit] = {
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").nn.toNanos)
    val t = Timeout(d)
    val fut = hash.ask(rng.Delete(stob(key)))(t).mapTo[rng.Ack]
    Try(Await.result(fut, d)) match {
      case Success(rng.AckSuccess(_)) => Right(())
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(rng.AckTimeoutFailed(op, k)) => Left(RngAskTimeoutFailed(op, k))
      case Failure(t) => Left(RngThrow(t))
    }
  }

  def save(path: String): Either[Err, String] = {
    val d = 1 hour
    val x = hash.ask(rng.Save(path))(Timeout(d))
    Try(Await.result(x, d)) match {
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(v: String) => Right(v)
      case Success(v) => Left(RngFail(s"Unexpected response: ${v}"))
      case Failure(t) => Left(RngThrow(t))
    }
  }
  def load(path: String): Either[Err, String] = {
    val d = Duration.fromNanos(cfg.getDuration("dump-timeout").nn.toNanos)
    val t = Timeout(d)
    val x = hash.ask(rng.Load(path))(t)
    Try(Await.result(x, d)) match {
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(v: String) => Right(v)
      case Success(v) => Left(RngFail(s"Unexpected response: ${v}"))
      case Failure(t) => Left(RngThrow(t))
    }
  }

  def nextid(feed: String): Either[Err, String] = {
    import akka.cluster.sharding.*
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").nn.toNanos)
    val t = Timeout(d)
    Try(Await.result(ClusterSharding(system).shardRegion(IdCounter.shardName).ask(feed)(t).mapTo[String],d)).toEither.leftMap(RngThrow.apply)
  }

  def compact(): Unit = {
    leveldb.compact()
  }

  def deleteByKeyPrefix(keyPrefix: Array[Byte]): Either[Err, Unit] = {
    val d = Duration.fromNanos(cfg.getDuration("iter-timeout").nn.toNanos)
    val t = Timeout(d)
    val x = hash.ask(rng.Iter(keyPrefix))(t)
    Try(Await.result(x, d)) match {
      case Success(rng.AckQuorumFailed(why)) => Left(RngAskQuorumFailed(why))
      case Success(res: zd.rng.IterRes) =>
        res.keys.foreach(log.info)
        res.keys.map(delete).sequence_
      case Success(v) => Left(RngFail(s"Unexpected response: ${v}"))
      case Failure(t) => Left(RngThrow(t))
    }
  }
}

object IdCounter {
  def props(kvs: WritableEl): Props = Props(new IdCounter(kvs))
  val shardName = "nextid"
}
class IdCounter(kvs: WritableEl) extends Actor with ActorLogging {
  given ElHandler[String] = new ElHandler[String] {
    def pickle(e: String): Array[Byte] = e.getBytes("utf8").nn
    def unpickle(a: Array[Byte]): String = String(a, "utf8").nn
  }

  def receive: Receive = {
    case name: String =>
      kvs.get[String](s"IdCounter.${name}").fold(
        l => log.error("can't get counter for name={} err={}", name, l)
      , r => r.cata(prev => put(name, prev), put(name, prev="0"))
      )
  }

  def put(name:String, prev: String): Unit = {
    kvs.put[String](s"IdCounter.$name", (prev.toLong+1).toString).fold(
      l => log.error(s"Failed to increment `$name` id=$l"),
      r => sender ! r
    )
  }
}
