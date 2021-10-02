package kvs
package store

import akka.actor.*
import akka.event.Logging
import akka.routing.FromConfig
import org.rocksdb.{util as _, *}
import proto.*
import rng.store.{ReadonlyStore, WriteStore}, rng.Hashing
import zio.*
import zio.clock.Clock

object Rng {
  import scala.concurrent.*, duration.*
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
  )
  def apply(as: ActorSystem, conf: Conf, clock: Clock.Service): Rng = new Rng(as, conf, clock)
}

class Rng(system: ActorSystem, conf: Rng.Conf, clock: Clock.Service) extends Dba with AutoCloseable {
  lazy val log = Logging(system, "hash-ring")

  val env = Has(clock)

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

  implicit val elkeyc: MessageCodec[ElKey] = caseCodecAuto
  implicit val fdkeyc: MessageCodec[FdKey] = caseCodecAuto
  implicit val enkeyc: MessageCodec[EnKey] = caseCodecAuto
  implicit val pathc: MessageCodec[PathKey]  = caseCodecAuto
  implicit val chunkc: MessageCodec[ChunkKey] = caseCodecAuto
  implicit val keyc: MessageCodec[Key] = sealedTraitCodecAuto

  private def withRetryOnce[A](op: Array[Byte] => A, key: Key): ZIO[Clock, Err, Option[Array[Byte]]] = {
    import zio.duration.*
    for {
      k  <- IO.effectTotal(encode[Key](key))
      x  <- ZIO.effectAsync { (callback: IO[Err, Option[Array[Byte]]] => Unit) =>
              val receiver = system.actorOf(Receiver.props{
                case Right(a) => callback(IO.succeed(a))
                case Left (e) => callback(IO.fail(e))
              })
              hash.tell(op(k), receiver)
            }.retry(Schedule.fromDuration(100 milliseconds))
    } yield x
  }.provide(env)

  override def put(key: Key, value: Array[Byte]): IO[Err, Unit] =
    withRetryOnce(rng.Put(_, value), key).unit.provide(env)

  override def get(key: Key): IO[Err, Option[Array[Byte]]] =
    withRetryOnce(rng.Get(_), key).provide(env)

  override def del(key: Key): IO[Err, Unit] =
    withRetryOnce(rng.Delete(_), key).unit.provide(env)

  def close(): Unit = {
    try{    db.close()}catch{case _:Throwable=>}
    try{dbopts.close()}catch{case _:Throwable=>}
  }
}

object Receiver {
  def props(cb: Res[Option[Array[Byte]]]=>Unit): Props = Props(new Receiver(cb))
}

class Receiver(cb: Res[Option[Array[Byte]]]=>Unit) extends Actor with ActorLogging {
  def receive: Receive = {
    case x: Ack =>
      val res = x match {
        case AckSuccess(v)       => Right(v)
        case x: AckQuorumFailed  => Left(AckFail(x))
        case x: AckTimeoutFailed => Left(AckFail(x))
      }
      cb(res)
      context.stop(self)
    case x =>
      log.error(x.toString)
      context.stop(self)
  }
}
