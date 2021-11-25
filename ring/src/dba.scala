package kvs.rng

import akka.actor.{Actor, ActorLogging, Props, Deploy}
import akka.event.Logging
import akka.routing.FromConfig
import org.rocksdb.{util as _, *}
import scala.language.postfixOps
import zio.*
import zio.clock.Clock

import proto.*

import kvs.rng.store.{ReadonlyStore, WriteStore}

/* Database API */

type Dba = Has[Dba.Service]

object Dba:
  trait Service extends AutoCloseable:
    def put(key: Key, value: Value): IO[AckReceiverErr, Unit]
    def get(key: Key): IO[AckReceiverErr, Option[Value]]
    def delete(key: Key): IO[AckReceiverErr, Unit]
  end Service

  val live: ZLayer[ActorSystem & Has[Conf] & Clock, Throwable, Dba] =
    ZLayer.fromManaged(
      ZManaged.fromAutoCloseable(
        for
          as <- ZIO.service[ActorSystem.Service]
          conf <- ZIO.service[Conf]
          clock <- ZIO.service[Clock.Service]
          dba <-
            ZIO.effect(
              new Service {
                lazy val log = Logging(as, "hash-ring")

                val env = Has(clock)

                as.eventStream

                RocksDB.loadLibrary()
                val dbopts = new Options().nn
                  .setCreateIfMissing(true).nn
                  .setCompressionType(CompressionType.LZ4_COMPRESSION).nn
                val db = RocksDB.open(dbopts, conf.dir).nn

                val hashing = Hashing(conf)
                as.actorOf(WriteStore.props(db, hashing).withDeploy(Deploy.local), name="ring_write_store")
                as.actorOf(FromConfig.props(ReadonlyStore.props(db, hashing)).withDeploy(Deploy.local), name="ring_readonly_store")

                val hash = as.actorOf(Hash.props(conf, hashing).withDeploy(Deploy.local), name="ring_hash")

                def put(key: Key, value: Value): IO[AckReceiverErr, Unit] =
                  withRetryOnce(Put(key, value)).unit.provide(env)

                def get(key: Key): IO[AckReceiverErr, Option[Value]] =
                  withRetryOnce(Get(key)).provide(env)

                def delete(key: Key): IO[AckReceiverErr, Unit] =
                  withRetryOnce(Delete(key)).unit.provide(env)

                private def withRetryOnce[A](v: => A): ZIO[Clock, AckReceiverErr, Option[Array[Byte]]] =
                  import zio.duration.*
                  ZIO.effectAsync{
                    (callback: IO[AckReceiverErr, Option[Array[Byte]]] => Unit) =>
                      val receiver = as.actorOf(AckReceiver.props{
                        case Right(a) => callback(IO.succeed(a))
                        case Left(e) => callback(IO.fail(e))
                      })
                      hash.tell(v, receiver)
                  }.retry(Schedule.fromDuration(100 milliseconds)).provide(env)

                def close(): Unit =
                  try { db.close() } catch { case _: Throwable => }
                  try { dbopts.close() } catch { case _: Throwable => }
              }
            )
        yield dba
      )
    )
end Dba

type AckReceiverErr = AckQuorumFailed | AckTimeoutFailed
type AckReceiverCallback = Either[AckReceiverErr, Option[Value]] => Unit

object AckReceiver:
  def props(cb: AckReceiverCallback): Props = Props(AckReceiver(cb))

class AckReceiver(cb: AckReceiverCallback) extends Actor with ActorLogging:
  def receive: Receive =
    case x: Ack =>
      val res = x match
        case AckSuccess(v) => Right(v)
        case x: AckQuorumFailed => Left(x)
        case x: AckTimeoutFailed => Left(x)
      cb(res)
      context.stop(self)
    case x =>
      log.error(x.toString)
      context.stop(self)
end AckReceiver
