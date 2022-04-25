package kvs.rng

import akka.actor.{Actor, ActorLogging, Props, Deploy}
import akka.event.Logging
import akka.routing.FromConfig
import kvs.rng.store.{ReadonlyStore, WriteStore}
import org.rocksdb.{util as _, *}
import proto.*
import scala.language.postfixOps
import zio.*

/* Database API */
trait Dba:
  def put(key: Key, value: Value): IO[DbaErr, Unit]
  def get(key: Key): IO[DbaErr, Option[Value]]
  def delete(key: Key): IO[DbaErr, Unit]
end Dba

type DbaErr = AckQuorumFailed | AckTimeoutFailed

object Dba:
  val live: ZLayer[ActorSystem & Conf & Clock, Throwable, Dba] =
    ZLayer.scoped(
      for
        as <- ZIO.service[ActorSystem]
        conf <- ZIO.service[Conf]
        clock <- ZIO.service[Clock]
        _ <- IO.attempt(RocksDB.loadLibrary())
        opts <-
          ZIO.fromAutoCloseable(
            ZIO.attempt{
              Options().nn
                .setCreateIfMissing(true).nn
                .setCompressionType(CompressionType.LZ4_COMPRESSION).nn
            }
          )
        db <-
          ZIO.fromAutoCloseable(
            ZIO.attempt(
              RocksDB.open(opts, conf.dir).nn
            )
          )
        _ <- ZIO.attempt(as.eventStream)
        dba <-
          ZIO.attempt(
            new Dba:
              val hashing = Hashing(conf)
              as.actorOf(WriteStore.props(db, hashing).withDeploy(Deploy.local), name="ring_write_store")
              as.actorOf(FromConfig.props(ReadonlyStore.props(db, hashing)).withDeploy(Deploy.local), name="ring_readonly_store")

              val hash = as.actorOf(Hash.props(conf, hashing).withDeploy(Deploy.local), name="ring_hash")

              def put(key: Key, value: Value): IO[DbaErr, Unit] =
                withRetryOnce(Put(key, value)).unit.provideEnvironment(ZEnvironment(clock))

              def get(key: Key): IO[DbaErr, Option[Value]] =
                withRetryOnce(Get(key)).provideEnvironment(ZEnvironment(clock))

              def delete(key: Key): IO[DbaErr, Unit] =
                withRetryOnce(Delete(key)).unit.provideEnvironment(ZEnvironment(clock))

              private def withRetryOnce[A](v: => A): ZIO[Clock, DbaErr, Option[Array[Byte]]] =
                ZIO.async{
                  (callback: IO[DbaErr, Option[Array[Byte]]] => Unit) =>
                    val receiver = as.actorOf(AckReceiver.props{
                      case Right(a) => callback(IO.succeed(a))
                      case Left(e) => callback(IO.fail(e))
                    })
                    hash.tell(v, receiver)
                }.retry(Schedule.fromDuration(100 milliseconds)).provideEnvironment(ZEnvironment(clock))
          )
      yield dba
    )
end Dba

type AckReceiverCallback = Either[DbaErr, Option[Value]] => Unit

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
