package mws.kvs
package store

import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.routing.FromConfig
import akka.util.{Timeout}
import leveldbjnr.LevelDB
import mws.kvs.el.ElHandler
import mws.rng
import mws.rng.store.{ReadonlyStore, WriteStore}
import mws.rng.{atob, stob}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scalaz._
import Scalaz._

class Ring(system: ActorSystem) extends Dba {
  lazy val log = Logging(system, "hash-ring")

  val d = 5 seconds
  implicit val timeout = Timeout(d)

  val config = system.settings.config

  lazy val clusterConfig = config.getConfig("akka.cluster")
  system.eventStream

  val leveldb: LevelDB = LeveldbOps.open(system, config.getString("ring.leveldb.dir"))

  system.actorOf(WriteStore.props(leveldb).withDeploy(Deploy.local), name="ring_write_store")
  system.actorOf(FromConfig.props(ReadonlyStore.props(leveldb)).withDeploy(Deploy.local), name="ring_readonly_store")

  val hash = system.actorOf(rng.Hash.props().withDeploy(Deploy.local), name="ring_hash")

  def put(key: String, value: V): Res[V] = {
    val putF = (hash ? rng.Put(stob(key), atob(value))).mapTo[rng.Ack]
    Try(Await.result(putF, d)) match {
      case Success(rng.AckSuccess(_)) => value.right
      case Success(rng.AckQuorumFailed(why)) => RngAskQuorumFailed(why).left
      case Success(rng.AckTimeoutFailed(on)) => RngAskTimeoutFailed(on).left
      case Failure(t) => RngThrow(t).left
    }
  }

  def isReady: Future[Boolean] = hash.ask(rng.Ready).mapTo[Boolean]

  def get(key: String): Res[V] = {
    val fut = hash.ask(rng.Get(stob(key))).mapTo[rng.Ack]
    Try(Await.result(fut, d)) match {
      case Success(rng.AckSuccess(Some(v))) => v.toByteArray.right
      case Success(rng.AckSuccess(None)) => NotFound(key).left
      case Success(rng.AckQuorumFailed(why)) => RngAskQuorumFailed(why).left
      case Success(rng.AckTimeoutFailed(on)) => RngAskTimeoutFailed(on).left
      case Failure(t) => RngThrow(t).left
    }
  }

  def delete(key: String): Res[V] =
    get(key).flatMap{ r =>
      val fut = hash.ask(rng.Delete(stob(key))).mapTo[rng.Ack]
      Try(Await.result(fut, d)) match {
        case Success(rng.AckSuccess(_)) => r.right
        case Success(rng.AckQuorumFailed(why)) => RngAskQuorumFailed(why).left
        case Success(rng.AckTimeoutFailed(on)) => RngAskTimeoutFailed(on).left
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
    val d = 1 hour
    val x = hash.ask(rng.Load(path, javaSer=false))(Timeout(d))
    Try(Await.result(x, d)) match {
      case Success(rng.AckQuorumFailed(why)) => RngAskQuorumFailed(why).left
      case Success(v: String) => v.right
      case Success(v) => RngFail(s"Unexpected response: ${v}").left
      case Failure(t) => RngThrow(t).left
    }
  }
  def loadJava(path: String): Res[Any] = {
    val d = 1 hour
    val x = hash.ask(rng.Load(path, javaSer=true))(Timeout(d))
    Try(Await.result(x, d)) match {
      case Success(rng.AckQuorumFailed(why)) => RngAskQuorumFailed(why).left
      case Success(v: String) => v.right
      case Success(v) => RngFail(s"Unexpected response: ${v}").left
      case Failure(t) => RngThrow(t).left
    }
  }
  def iterate(path: String, f: (String, Array[Byte]) => Unit): Res[Any] = {
    val d = 1 hour
    val x = hash.ask(rng.Iterate(path, (k, v) => f(new String(k.toByteArray, "UTF-8"), v.toByteArray)))(Timeout(d))
    Try(Await.result(x, d)) match {
      case Success(rng.AckQuorumFailed(why)) => RngAskQuorumFailed(why).left
      case Success(v: String) => v.right
      case Success(v) => RngFail(s"Unexpected response: ${v}").left
      case Failure(t) => RngThrow(t).left
    }
  }

  def nextid(feed: String): Res[String] = {
    import akka.cluster.sharding._
    Try(Await.result(ClusterSharding(system).shardRegion(IdCounter.shardName).ask(feed).mapTo[String],d)).toDisjunction.leftMap(RngThrow)
  }

  def compact(): Unit = {
    leveldb.compactRange(null, null)
  }
}

object IdCounter {
  def props: Props = Props(new IdCounter)
  val shardName = "nextid"
}
class IdCounter extends Actor with ActorLogging {
  val kvs = mws.kvs.Kvs(context.system)

  implicit val strHandler: ElHandler[String] = new ElHandler[String] {
    def pickle(e: String): Res[Array[Byte]] = e.getBytes("UTF-8").right
    def unpickle(a: Array[Byte]): Res[String] = new String(a,"UTF-8").right
  }

  def receive: Receive = {
    case name: String =>
      kvs.el.get[String](s"IdCounter.${name}").fold(
        empty => put(name, prev="0"),
        prev => put(name, prev)
      )
  }

  def put(name:String, prev: String): Unit = {
    kvs.el.put[String](s"IdCounter.$name", (prev.toLong+1).toString).fold(
      l => log.error(s"Failed to increment `$name` id=$l"),
      r => sender ! r
    )
  }
}
