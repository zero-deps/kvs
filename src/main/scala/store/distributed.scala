package mws
package kvs
package store

import scala.language.postfixOps
import scala.concurrent.{Await,Future}
import scala.concurrent.duration._
import scala.util.Try

import scalaz._, Scalaz._, Maybe.{Empty, Just}

import akka.event.Logging
import akka.routing.FromConfig
import akka.pattern.ask
import akka.actor._
import akka.util.{Timeout,ByteString}

import mws.rng._
import mws.rng.store.{ReadonlyStore,WriteStore}

object Ring {
  def apply(system: ActorSystem): Dba = new Ring(system)

  def openLeveldb(s: ActorSystem, path: Maybe[String]=Empty()) = {
    import com.protonail.leveldb.jna._
    val config = s.settings.config.getConfig("ring.leveldb")
    val leveldbDir = path.getOrElse(config.getString("dir"))
    val leveldbOptions = new LevelDBOptions() {
      val bloom = native.leveldb_filterpolicy_create_bloom(10)
      native.leveldb_options_set_filter_policy(options, bloom)
      val cache = native.leveldb_cache_create_lru(100 * 1048576) // 100MB cache
      native.leveldb_options_set_cache(options, cache)
    }
    leveldbOptions.setCreateIfMissing(true)
    new LevelDB(leveldbDir, leveldbOptions)
  }
}

class Ring(system: ActorSystem) extends Dba {
  import Ring._
  lazy val log = Logging(system, "hash-ring")

  val d = 5 seconds
  implicit val timeout = Timeout(d)

  lazy val clusterConfig = system.settings.config.getConfig("akka.cluster")
  system.eventStream

  var leveldb = openLeveldb(system)

  system.actorOf(Props(classOf[WriteStore],leveldb).withDeploy(Deploy.local), name="ring_write_store")
  system.actorOf(FromConfig.props(Props(classOf[ReadonlyStore], leveldb)).withDeploy(Deploy.local), name = "ring_readonly_store")

  private val hash = system.actorOf(Props(classOf[Hash]).withDeploy(Deploy.local), name = "ring_hash")

  def put(key: String, value: V): Res[V] = {
    val putF = (hash ? Put(key, ByteString(value))).mapTo[Ack]
    Try(Await.result(putF, d)).toDisjunction match {
      case \/-(AckSuccess) => value.right
      case \/-(AckQuorumFailed) => RngAskQuorumFailed.left
      case \/-(AckTimeoutFailed) => RngAskTimeoutFailed.left
      case -\/(ex) => RngThrow(ex).left
    }
  }

  def isReady: Future[Boolean] = (hash ? Ready).mapTo[Boolean]

  def get(key: String): Res[V] = {
    val getF = (hash ? Get(key)).mapTo[Option[rng.Value]]
    Try(Await.result(getF, d)).toDisjunction.map(_.toMaybe) match {
      case \/-(Just(v)) => v.toArray.right
      case \/-(Empty()) => NotFound(key).left
      case -\/(ex) => RngThrow(ex).left
    }
  }

  def delete(key: String): Res[V] =
    get(key).flatMap{ r =>
      Try(Await.result((hash ? Delete(key)).mapTo[Ack], d)).toDisjunction match {
        case \/-(AckSuccess) => r.right
        case \/-(AckQuorumFailed) => RngAskQuorumFailed.left
        case \/-(AckTimeoutFailed) => RngAskTimeoutFailed.left
        case -\/(ex) => RngThrow(ex).left
      }
    }

  def save():Future[String] = (hash ? Dump).mapTo[String]
  def load(path:String):Future[Any] = hash ? LoadDump(path)
  def iterate(path:String,foreach:(String,Array[Byte])=>Unit):Future[Any] = hash ? IterateDump(path,foreach)

  def close(): Unit = ()

  def nextid(feed:String):Res[String] = {
    import akka.cluster.sharding._
    Try(Await.result(ClusterSharding(system).shardRegion(IdCounter.shardName).ask(feed).mapTo[String],d)).toDisjunction.leftMap(RngThrow(_))
  }
}

object IdCounter {
  def props:Props = Props(new IdCounter)
  val shardName = "nextid"
}
class IdCounter extends Actor with ActorLogging {
  val kvs = mws.kvs.Kvs(context.system)

  import mws.kvs.handle.ElHandler
  implicit val strHandler:ElHandler[String] = new ElHandler[String] {
    def pickle(e: String): Array[Byte] = e.getBytes("UTF-8")
    def unpickle(a: Array[Byte]): Res[String] = new String(a,"UTF-8").right
  }

  def receive: Receive = {
    case name:String =>
      kvs.get[String](s"IdCounter.${name}").fold(
        empty => put(name, prev="0"),
        prev => put(name, prev)
      )
  }

  def put(name:String, prev: String): Unit = {
    kvs.put[String](s"IdCounter.$name", (prev.toLong+1).toString).fold(
      l => log.error(s"Failed to increment `$name` id=$l"),
      r => sender ! r
    )
  }
}
