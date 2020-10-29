package zd.kvs
package store

import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.routing.FromConfig
import akka.util.{Timeout}
import leveldbjnr.LevelDb
import zd.kvs.el.ElHandler
import zd.rng
import zd.rng.store.{ReadonlyStore, WriteStore}
import zd.rng.stob
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import zero.ext._, either._, option._

class Rng(system: ActorSystem) extends Dba {
  lazy val log = Logging(system, "hash-ring")

  val cfg = system.settings.config.getConfig("ring")

  system.eventStream

  val leveldbPath = cfg.getString("leveldb.dir")
  val leveldb: LevelDb = LevelDb.open(leveldbPath).fold(l => throw l, r => r)

  system.actorOf(WriteStore.props(leveldb).withDeploy(Deploy.local), name="ring_write_store")
  system.actorOf(FromConfig.props(ReadonlyStore.props(leveldb)).withDeploy(Deploy.local), name="ring_readonly_store")

  val hash = system.actorOf(rng.Hash.props().withDeploy(Deploy.local), name="ring_hash")

  def put(key: String, value: V): Res[V] = {
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").toNanos)
    val t = Timeout(d)
    val putF = hash.ask(rng.Put(stob(key), value))(t).mapTo[rng.Ack]
    Try(Await.result(putF, d)) match {
      case Success(rng.AckSuccess(_)) => value.right
      case Success(rng.AckQuorumFailed(why)) => RngAskQuorumFailed(why).left
      case Success(rng.AckTimeoutFailed(op, k)) => RngAskTimeoutFailed(op, k).left
      case Failure(t) => RngThrow(t).left
    }
  }

  private def isReady(): Future[Boolean] = {
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").toNanos)
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

  def get(key: String): Res[Option[V]] = {
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").toNanos)
    val t = Timeout(d)
    val fut = hash.ask(rng.Get(stob(key)))(t).mapTo[rng.Ack]
    Try(Await.result(fut, d)) match {
      case Success(rng.AckSuccess(v)) => v.right
      case Success(rng.AckQuorumFailed(why)) => RngAskQuorumFailed(why).left
      case Success(rng.AckTimeoutFailed(op, k)) => RngAskTimeoutFailed(op, k).left
      case Failure(t) => RngThrow(t).left
    }
  }

  def delete(key: String): Res[Unit] = {
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").toNanos)
    val t = Timeout(d)
    val fut = hash.ask(rng.Delete(stob(key)))(t).mapTo[rng.Ack]
    Try(Await.result(fut, d)) match {
      case Success(rng.AckSuccess(_)) => ().right
      case Success(rng.AckQuorumFailed(why)) => RngAskQuorumFailed(why).left
      case Success(rng.AckTimeoutFailed(op, k)) => RngAskTimeoutFailed(op, k).left
      case Failure(t) => RngThrow(t).left
    }
  }

  def save(path: String): Res[String] = {
    val d = 1 hour
    val x = hash.ask(rng.Save(path))(Timeout(d))
    Try(Await.result(x, d)) match {
      case Success(rng.AckQuorumFailed(why)) => RngAskQuorumFailed(why).left
      case Success(v: String) => v.right
      case Success(v) => RngFail(s"Unexpected response: ${v}").left
      case Failure(t) => RngThrow(t).left
    }
  }
  def load(path: String): Res[Any] = {
    val d = Duration.fromNanos(cfg.getDuration("dump-timeout").toNanos)
    val t = Timeout(d)
    val x = hash.ask(rng.Load(path))(t)
    Try(Await.result(x, d)) match {
      case Success(rng.AckQuorumFailed(why)) => RngAskQuorumFailed(why).left
      case Success(v: String) => v.right
      case Success(v) => RngFail(s"Unexpected response: ${v}").left
      case Failure(t) => RngThrow(t).left
    }
  }

  def nextid(feed: String): Res[String] = {
    import akka.cluster.sharding._
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").toNanos)
    val t = Timeout(d)
    Try(Await.result(ClusterSharding(system).shardRegion(IdCounter.shardName).ask(feed)(t).mapTo[String],d)).toEither.leftMap(RngThrow)
  }

  def compact(): Unit = {
    leveldb.compact()
  }
}

object IdCounter {
  def props(kvs: ElApi): Props = Props(new IdCounter(kvs))
  val shardName = "nextid"
}
class IdCounter(kvs: ElApi) extends Actor with ActorLogging {
  implicit val strHandler: ElHandler[String] = new ElHandler[String] {
    def pickle(e: String): Res[Array[Byte]] = e.getBytes("UTF-8").right
    def unpickle(a: Array[Byte]): Res[String] = new String(a,"UTF-8").right
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
