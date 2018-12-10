package mws.kvs
package store

import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.routing.FromConfig
import akka.util.{Timeout}
import akka.actor._
import akka.util.Timeout
import leveldbjnr.LevelDB
import mws.kvs.el.ElHandler
import mws.rng
import mws.rng.{atob, stob}
import mws.rng.store.{ReadonlyStore, WriteStore}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Try
import scalaz._
import Scalaz._

object Ring {
  def apply(system: ActorSystem): Dba = new Ring(system)

  def openLeveldb(s: ActorSystem, path: Option[String]=None): LevelDB = {
    import leveldbjnr._
    val config = s.settings.config.getConfig("ring.leveldb")
    val leveldbDir: String = path.getOrElse(config.getString("dir"))
    val leveldbOptions = new LevelDBOptions() {
      val bloom = LevelDB.lib.leveldb_filterpolicy_create_bloom(10)
      LevelDB.lib.leveldb_options_set_filter_policy(options, bloom)
      val cache = LevelDB.lib.leveldb_cache_create_lru(500 * 1048576) // 100MB cache
      LevelDB.lib.leveldb_options_set_cache(options, cache)
      val writeBuffer = 200 * 1048576 // 10MB write buffer
      LevelDB.lib.leveldb_options_set_write_buffer_size(options, writeBuffer)
      // val fileSize = 50 * 1048576
      // LevelDB.lib.leveldb_options_set_max_file_size(options, fileSize)
    }
    leveldbOptions.setWriteBufferSize(200 * 1048576)
    leveldbOptions.setBlockSize(65536)
    leveldbOptions.setMaxOpenFiles(2500)
    leveldbOptions.setCreateIfMissing(true)
    val x = new LevelDB(leveldbDir, leveldbOptions)
    leveldbOptions.close()
    x
  }
}

class Ring(system: ActorSystem) extends Dba {
  import Ring._
  lazy val log = Logging(system, "hash-ring")

  val d = 5 seconds
  implicit val timeout = Timeout(d)

  lazy val clusterConfig = system.settings.config.getConfig("akka.cluster")
  system.eventStream

  var leveldb: LevelDB = openLeveldb(system)

  system.actorOf(Props(classOf[WriteStore],leveldb).withDeploy(Deploy.local), name="ring_write_store")
  system.actorOf(FromConfig.props(Props(classOf[ReadonlyStore], leveldb)).withDeploy(Deploy.local), name = "ring_readonly_store")

  private val hash = system.actorOf(Props(classOf[rng.Hash]).withDeploy(Deploy.local), name = "ring_hash")

  def put(key: String, value: V): Res[V] = {
    val putF = (hash ? rng.Put(stob(key), atob(value))).mapTo[rng.Ack]
    Try(Await.result(putF, d)).toDisjunction match {
      case \/-(rng.AckSuccess) => value.right
      case \/-(rng.AckQuorumFailed) => RngAskQuorumFailed.left
      case \/-(rng.AckTimeoutFailed) => RngAskTimeoutFailed.left
      case -\/(t) => RngThrow(t).left
    }
  }

  def isReady: Future[Boolean] = hash.ask(rng.Ready).mapTo[Boolean]

  def get(key: String): Res[V] = {
    val getF = (hash ? rng.Get(stob(key))).mapTo[Option[rng.Value]]
    Try(Await.result(getF, d)).toDisjunction match {
      case \/-(Some(v)) => v.toByteArray.right
      case \/-(None) => NotFound(key).left
      case -\/(t) => RngThrow(t).left
    }
  }

  def delete(key: String): Res[V] =
    get(key).flatMap{ r =>
      Try(Await.result((hash ? rng.Delete(stob(key))).mapTo[rng.Ack], d)).toDisjunction match {
        case \/-(rng.AckSuccess) => r.right
        case \/-(rng.AckQuorumFailed) => RngAskQuorumFailed.left
        case \/-(rng.AckTimeoutFailed) => RngAskTimeoutFailed.left
        case -\/(t) => RngThrow(t).left
      }
    }

  def save(path: String): Future[String] = (hash.ask(rng.Dump(path))(Timeout(1 hour))).mapTo[String]
  def load(path: String): Future[Any] = hash.ask(rng.LoadDump(path, javaSer=false))(Timeout(1 hour))
  def loadJava(path: String): Future[Any] = hash.ask(rng.LoadDump(path, javaSer=true))(Timeout(1 hour))
  def iterate(path:String, foreach: (String, Array[Byte]) => Unit): Future[Any] = hash.ask(rng.IterateDump(path, (k, v) => foreach(new String(k.toByteArray, "UTF-8"), v.toByteArray)))(Timeout(1 hour))

  def nextid(feed:String):Res[String] = {
    import akka.cluster.sharding._
    Try(Await.result(ClusterSharding(system).shardRegion(IdCounter.shardName).ask(feed).mapTo[String],d)).toDisjunction.leftMap(RngThrow)
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
