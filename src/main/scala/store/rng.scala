package kvs
package store

import akka.actor._
import akka.event.Logging
import akka.routing.FromConfig
import org.rocksdb.{util=>_,_}
import scala.concurrent._, duration._
import zero.ext._, either._
import zd.proto._, api._, macrosapi._
import zio._

import rng.store.{ReadonlyStore, WriteStore}, rng.Hashing

object Rng {
  case class Quorum(N: Int, W: Int, R: Int)
  case class Conf(
    quorum: Quorum = Quorum(N=1, W=1, R=1)
  , buckets:      Int = 32768 /* 2^15 */
  , virtualNodes: Int =   128
  , hashLength:   Int =    32
  , ringTimeout:   FiniteDuration = 11 seconds /* bigger than gatherTimeout */
  , gatherTimeout: FiniteDuration = 10 seconds
  , dumpTimeout:   FiniteDuration =  1 hour
  , replTimeout:   FiniteDuration =  1 minute
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
  val dbopts = new Options()
    .setCreateIfMissing(true)
    .setCompressionType(CompressionType.LZ4_COMPRESSION)
  val db = RocksDB.open(dbopts, conf.dir)

  val hashing = new Hashing(conf)
  system.actorOf(WriteStore.props(db, hashing).withDeploy(Deploy.local), name="ring_write_store")
  system.actorOf(FromConfig.props(ReadonlyStore.props(db, hashing)).withDeploy(Deploy.local), name="ring_readonly_store")

  val hash = system.actorOf(rng.Hash.props(conf, hashing).withDeploy(Deploy.local), name="ring_hash")

  implicit val elkeyc = caseCodecAuto[ElKey]
  implicit val fdkeyc = caseCodecAuto[FdKey]
  implicit val enkeyc = caseCodecAuto[EnKey]
  implicit val pathc  = caseCodecAuto[PathKey]
  implicit val chunkc = caseCodecAuto[ChunkKey]
  implicit val keyc   = sealedTraitCodecAuto[Key]

  override def put(key: Key, value: Bytes): KIO[Unit] = {
    ZIO.effectAsync { callback =>
      val receiver = system.actorOf(Receiver.props{
        case Right(_) => callback(IO.succeed(()))
        case Left (e) => callback(IO.fail(e))
      })
      hash.tell(rng.Put(encodeToBytes[Key](key), value), receiver)
    }
  }

  override def get(key: Key): KIO[Option[Bytes]] = {
    ZIO.effectAsync { callback =>
      val receiver = system.actorOf(Receiver.props{
        case Right(a) => callback(IO.succeed(a))
        case Left (e) => callback(IO.fail(e))
      })
      hash.tell(rng.Get(encodeToBytes[Key](key)), receiver)
    }
  }

  override def del(key: Key): KIO[Unit] = {
    ZIO.effectAsync { callback =>
      val receiver = system.actorOf(Receiver.props{
        case Right(_) => callback(IO.succeed(()))
        case Left (e) => callback(IO.fail(e))
      })
      hash.tell(rng.Delete(encodeToBytes[Key](key)), receiver)
    }
  }

  def close(): Unit = {
    db.close()
    dbopts.close()
  }
}

object Receiver {
  def props(cb: Res[Option[Bytes]]=>Unit): Props = Props(new Receiver(cb))
}

class Receiver(cb: Res[Option[Bytes]]=>Unit) extends Actor with ActorLogging {
  def receive: Receive = {
    case x: Ack =>
      val res = x match {
        case AckSuccess(v)       => v.right
        case x: AckQuorumFailed  => AckFail(x).left
        case x: AckTimeoutFailed => AckFail(x).left
      }
      cb(res)
      context.stop(self)
    case x =>
      log.error(x.toString)
      context.stop(self)
  }
}
