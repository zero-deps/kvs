package kvs
package store

import akka.actor._
import akka.event.Logging
import akka.routing.FromConfig
import org.rocksdb.{util=>_,_}
import proto._, macrosapi._
import rng.store.{ReadonlyStore, WriteStore}, rng.Hashing
import zio._
import zio.clock.Clock

object Rng {
  import scala.concurrent._, duration._
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
  def apply(as: ActorSystem, conf: Conf, clock: Clock.Service): Rng = new Rng(as, conf, clock)
}

class Rng(system: ActorSystem, conf: Rng.Conf, clock: Clock.Service) extends Dba with AutoCloseable {
  lazy val log = Logging(system, "hash-ring")

  val env = Has(clock)

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

  private def withRetryOnce[A](op: Bytes => A, key: Key): ZIO[Clock, Err, Option[Bytes]] = {
    import zio.duration._
    for {
      k  <- IO.effectTotal(encodeToBytes[Key](key))
      x  <- ZIO.effectAsync { callback: (IO[Err, Option[Bytes]] => Unit) =>
              val receiver = system.actorOf(Receiver.props{
                case Right(a) => callback(IO.succeed(a))
                case Left (e) => callback(IO.fail(e))
              })
              hash.tell(op(k), receiver)
            }.retry(Schedule.fromDuration(100 milliseconds))
    } yield x
  }.provide(env)

  override def put(key: Key, value: Bytes): IO[Err, Unit] =
    withRetryOnce(rng.Put(_, value), key).unit.provide(env)

  override def get(key: Key): IO[Err, Option[Bytes]] =
    withRetryOnce(rng.Get(_), key).provide(env)

  override def del(key: Key): IO[Err, Unit] =
    withRetryOnce(rng.Delete(_), key).unit.provide(env)

  def close(): Unit = {
    try{    db.close()}catch{case _:Throwable=>}
    try{dbopts.close()}catch{case _:Throwable=>}
  }
}

object Receiver {
  def props(cb: Res[Option[Bytes]]=>Unit): Props = Props(new Receiver(cb))
}

class Receiver(cb: Res[Option[Bytes]]=>Unit) extends Actor with ActorLogging {
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
