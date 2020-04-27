package zd.kvs
package store

import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.routing.FromConfig
import akka.util.{Timeout}
import leveldbjnr.LevelDb
import zd.kvs.rng
import zd.kvs.rng.store.{ReadonlyStore, WriteStore}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Try, Success, Failure}
import zero.ext._, either._
import zd.proto.Bytes

class Ring(system: ActorSystem) extends Dba {
  lazy val log = Logging(system, "hash-ring")

  val cfg = system.settings.config.getConfig("ring")

  system.eventStream

  val leveldb: LevelDb = LevelDb.open(cfg.getString("leveldb.dir")).fold(l => throw l, r => r)

  system.actorOf(WriteStore.props(leveldb).withDeploy(Deploy.local), name="ring_write_store")
  system.actorOf(FromConfig.props(ReadonlyStore.props(leveldb)).withDeploy(Deploy.local), name="ring_readonly_store")

  val hash = system.actorOf(rng.Hash.props().withDeploy(Deploy.local), name="ring_hash")

  override def put(key: Bytes, value: Bytes): Res[Unit] = {
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").toNanos)
    val t = Timeout(d)
    val putF = hash.ask(rng.Put(key, value))(t).mapTo[Ack]
    Try(Await.result(putF, d)) match {
      case Success(AckSuccess(_)) => ().right
      case Success(x: AckQuorumFailed) => x.left
      case Success(x: AckTimeoutFailed) => x.left
      case Failure(t) => Throwed(t).left
    }
  }

  override def isReady: Future[Boolean] = {
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").toNanos)
    val t = Timeout(d)
    hash.ask(rng.Ready)(t).mapTo[Boolean]
  }

  override def get(key: Bytes): Res[Option[Bytes]] = {
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").toNanos)
    val t = Timeout(d)
    val fut = hash.ask(rng.Get(key))(t).mapTo[Ack]
    Try(Await.result(fut, d)) match {
      case Success(AckSuccess(v)) => v.right
      case Success(x: AckQuorumFailed) => x.left
      case Success(x: AckTimeoutFailed) => x.left
      case Failure(t) => Throwed(t).left
    }
  }

  override def delete(key: Bytes): Res[Unit] = {
    val d = Duration.fromNanos(cfg.getDuration("ring-timeout").toNanos)
    val t = Timeout(d)
    val fut = hash.ask(rng.Delete(key))(t).mapTo[Ack]
    Try(Await.result(fut, d)) match {
      case Success(AckSuccess(_)) => ().right
      case Success(x: AckQuorumFailed) => x.left
      case Success(x: AckTimeoutFailed) => x.left
      case Failure(t) => Throwed(t).left
    }
  }

  override def save(path: String): Res[String] = {
    val d = 1 hour
    val x = hash.ask(rng.Save(path))(Timeout(d))
    Try(Await.result(x, d)) match {
      case Success(x: AckQuorumFailed) => x.left
      case Success(v: String) => v.right
      case Success(v) => Fail(s"Unexpected response: ${v}").left
      case Failure(t) => Throwed(t).left
    }
  }

  override def load(path: String): Res[Any] = {
    val d = Duration.fromNanos(cfg.getDuration("dump-timeout").toNanos)
    val t = Timeout(d)
    val x = hash.ask(rng.Load(path))(t)
    Try(Await.result(x, d)) match {
      case Success(x: AckQuorumFailed) => x.left
      case Success(v: String) => v.right
      case Success(v) => Fail(s"Unexpected response: ${v}").left
      case Failure(t) => Throwed(t).left
    }
  }

  override def compact(): Unit = {
    leveldb.compact()
  }
}
