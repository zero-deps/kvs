package kvs

import org.apache.pekko.actor.{Actor, ActorLogging, Props, Deploy}
import org.apache.pekko.routing.FromConfig
import zd.rng.*, model.*, QuorumState.*
import org.rocksdb.{util as _, *}
import zio.*

object Rng:
  val layer: RLayer[ActorSystem & Conf, Dba] =
    ZLayer
      .scoped:
        for
          as <- ZIO.service[ActorSystem]
          conf <- ZIO.service[Conf]
          _ <- ZIO.attempt(RocksDB.loadLibrary())
          opts <-
            ZIO.fromAutoCloseable:
              ZIO.attempt:
                Options().nn
                  .setCreateIfMissing(true).nn
                  .setCompressionType(CompressionType.LZ4_COMPRESSION).nn
          db <-
            ZIO.fromAutoCloseable:
              ZIO.attempt:
                RocksDB.open(opts, conf.dir).nn
          _ <- ZIO.attempt(as.eventStream)
          dba <-
            ZIO.attempt:
              new Dba:
                val hashing = Hashing(conf)
                as.actorOf(WriteStore.props(db, hashing).withDeploy(Deploy.local), name="ring_write_store")
                as.actorOf(FromConfig.props(ReadonlyStore.props(db, hashing)).withDeploy(Deploy.local), name="ring_readonly_store")

                given CanEqual[Node, Node] = CanEqual.derived
                given CanEqual[QuorumStateUnsatisfied.type, QuorumState] = CanEqual.derived
                given CanEqual[ReplBucketUpToDate.type, Any] = CanEqual.derived
                given CanEqual[RestoreState.type, Any] = CanEqual.derived
                given CanEqual[String, Any] = CanEqual.derived
                val hash = as.actorOf(Hash.props(conf, hashing).withDeploy(Deploy.local), name="ring_hash")

                def put(key: Key, value: Value): UIO[Unit] =
                  withRetryOnce(Put(key, value)).unit

                def get(key: Key): UIO[Option[Value]] =
                  withRetryOnce(Get(key))

                def delete(key: Key): UIO[Unit] =
                  withRetryOnce(Delete(key)).unit

                private def withRetryOnce[A](v: => A): UIO[Option[Array[Byte]]] =
                  ZIO
                    .async:
                      (callback: IO[DbaErr, Option[Array[Byte]]] => Unit) =>
                        hash.tell(
                          v
                        , as.actorOf:
                            AckReceiver.props:
                              case Right(a) => callback(ZIO.succeed(a))
                              case Left(e) => callback(ZIO.fail(e))
                        )
                    .retry(Schedule.fromDuration(100.milliseconds))
                    .orDieWith(e => RuntimeException(e.toString))
        yield dba
      .orDie
end Rng

type AckReceiverCallback = Either[DbaErr, Option[Value]] => Unit

object AckReceiver:
  def props(cb: AckReceiverCallback): Props = Props(AckReceiver(cb))

class AckReceiver(cb: AckReceiverCallback) extends Actor with ActorLogging:
  def receive: Receive =
    case x: Ack =>
      val res = x match
        case AckSuccess(v) => Right(v)
        case x: (AckQuorumFailed | AckTimeoutFailed) => Left(x)
      cb(res)
      context.stop(self)
    case e =>
      log.error("unexpected response", e)
      context.stop(self)
end AckReceiver
