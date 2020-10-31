package kvs
package store

import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.routing.FromConfig
import akka.util.Timeout
import org.rocksdb.{util=>_,_}
import scala.concurrent._, duration._
import scala.concurrent.{Await, Future}
import scala.util.{Try, Success, Failure}
import zero.ext._, either._
import zd.proto._, api._, macrosapi._

import rng.store.{ReadonlyStore, WriteStore}, rng.Hashing

object Rng {
  case class Quorum(N: Int, W: Int, R: Int)
  case class Conf(
    quorum: Quorum = Quorum(N=1, W=1, R=1)
  , buckets:      Int = 32768 /* 2^15 */
  , virtualNodes: Int = 128
  , hashLength:   Int = 32
  , ringTimeout:   FiniteDuration = 11 seconds /* bigger than gatherTimeout */
  , gatherTimeout: FiniteDuration = 10 seconds
  , dumpTimeout:   FiniteDuration = 1 hour
  , replTimeout:   FiniteDuration = 1 minute
  , dir: String = "data_rng"
  , jmx: Boolean = true
  )
  def apply(as: ActorSystem, conf: Conf): Rng = new Rng(as, conf)
}

class Rng(system: ActorSystem, conf: Rng.Conf) extends Dba with AutoCloseable {
  lazy val log = Logging(system, "hash-ring")

  if (conf.jmx) {
    val jmx = new KvsJmx(this)
    jmx.createMBean()
    sys.addShutdownHook(jmx.unregisterMBean())
  }

  system.eventStream

  RocksDB.loadLibrary()
  val dbopts = new Options().setCreateIfMissing(true)
  val db = RocksDB.open(dbopts, conf.dir)

  val hashing = new Hashing(conf)
  system.actorOf(WriteStore.props(db, hashing).withDeploy(Deploy.local), name="ring_write_store")
  system.actorOf(FromConfig.props(ReadonlyStore.props(db, hashing)).withDeploy(Deploy.local), name="ring_readonly_store")

  val hash = system.actorOf(rng.Hash.props(conf, hashing).withDeploy(Deploy.local), name="ring_hash")

  implicit val elkeyc = caseCodecAuto[ElKey]
  implicit val fdkeyc = caseCodecAuto[FdKey]
  implicit val enkeyc = caseCodecAuto[EnKey]
  implicit val pathc = caseCodecAuto[PathKey]
  implicit val chunkc = caseCodecAuto[ChunkKey]
  implicit val keyc = sealedTraitCodecAuto[Key]

  override def put(key1: Key, value: Bytes): Res[Unit] = {
    val key = encodeToBytes[Key](key1)
    val d = conf.ringTimeout
    val t = Timeout(d)
    val putF = hash.ask(rng.Put(key, value))(t).mapTo[Ack]
    Try(Await.result(putF, d)) match {
      case Success(AckSuccess(_)) => ().right
      case Success(x: AckQuorumFailed) => x.left
      case Success(x: AckTimeoutFailed) => x.left
      case Failure(t) => Throwed(t).left
    }
  }

  override def get(key1: Key): Res[Option[Bytes]] = {
    val key = encodeToBytes[Key](key1)
    val d = conf.ringTimeout
    val t = Timeout(d)
    val fut = hash.ask(rng.Get(key))(t).mapTo[Ack]
    Try(Await.result(fut, d)) match {
      case Success(AckSuccess(v)) => v.right
      case Success(x: AckQuorumFailed) => x.left
      case Success(x: AckTimeoutFailed) => x.left
      case Failure(t) => Throwed(t).left
    }
  }

  override def delete(key1: Key): Res[Unit] = {
    val key = encodeToBytes[Key](key1)
    val d = conf.ringTimeout
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
    val d = conf.dumpTimeout
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
    db.compactRange()
  }

  private def isReady(): Future[Boolean] = {
    val d = conf.ringTimeout
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

  def close(): Unit = {
    db.close()
    dbopts.close()
  }
}
