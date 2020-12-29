package kvs.seq

import kvs.{FdKey, Res}
import zd.proto.api.{MessageCodec, encodeToBytes, decode}
import zd.proto.Bytes
import zio._
import zio.stream.Stream
import zio.akka.cluster.sharding.{Sharding, Entity}
import zio.macros.accessible
import _root_.akka.actor.{Actor, ActorLogging, Props}

@accessible
object KvsCircular {
  trait Service {
    def all[Bid, A](fid: Bid                 )(implicit i: Buffer[Bid, A]): KStream[A]
    def add[Bid, A](fid: Bid,            a: A)(implicit i: Buffer[Bid, A]):     KIO[Unit]
    def put[Bid, A](fid: Bid, idx: Long, a: A)(implicit i: Buffer[Bid, A]):     KIO[Unit]
    def get[Bid, A](fid: Bid, idx: Long      )(implicit i: Buffer[Bid, A]):     KIO[Option[A]]
  }

  val live: RLayer[ActorSystem with Dba with ZEnv, KvsCircular] = ZLayer.fromEffect {
    for {
      dba <- ZIO.service[Dba.Service]
      as  <- ZIO.environment[ActorSystem with ZEnv]
      sh  <- Sharding.start("kvs_circular_write_shard", Shard.onMessage(dba)).provide(as)
    } yield {
      new Service {
        private implicit val dba1 = dba

        def all[Bid, A](fid: Bid)(implicit i: Buffer[Bid, A]): KStream[A] = {
          for {
            fdKey <- Stream.fromEffect(i.fdKey(fid))
            xs    <- kvs.circular.all(fdKey).mapError(KvsErr(_)).mapM(i.data)
          } yield xs
        }

        def add[Bid, A](fid: Bid, a: A)(implicit i: Buffer[Bid, A]): KIO[Unit] = {
          for {
            fdKey  <- i.fdKey(fid)
            bytes  <- i.bytes(a)
            _      <- ZIO.effectAsyncM { (callback: KIO[Unit] => Unit) =>
                        val receiver = as.get.actorOf(Receiver.props{
                          case Right(_) => callback(IO.succeed(()))
                          case Left (e) => callback(IO.fail(KvsErr(e)))
                        })
                        sh.send(hex(fdKey.bytes), Shard.Add(fdKey, size=i.size, bytes), receiver)
                      }.mapError(ShardErr(_))
          } yield ()
        }

        def put[Bid, A](fid: Bid, idx: Long, a: A)(implicit i: Buffer[Bid, A]): KIO[Unit] =
          for {
            fdKey  <- i.fdKey(fid)
            bytes  <- i.bytes(a)
            _      <- ZIO.effectAsyncM { (callback: KIO[Unit] => Unit) =>
                        val receiver = as.get.actorOf(Receiver.props{
                          case Right(_) => callback(IO.succeed(()))
                          case Left (e) => callback(IO.fail(KvsErr(e)))
                        })
                        sh.send(hex(fdKey.bytes), Shard.Put(fdKey, idx=idx, bytes), receiver)
                      }.mapError(ShardErr(_))
          } yield ()

        def get[Bid, A](fid: Bid, idx: Long)(implicit i: Buffer[Bid, A]): KIO[Option[A]] =
          for {
            fdKey  <- i.fdKey(fid)
            res    <- kvs.circular.get(fdKey)(idx).mapError(KvsErr(_))
            a      <- (for {
                        bytes <- ZIO.fromOption(res)
                        a     <- i.data(bytes).mapError(Some(_))
                      } yield a).optional
          } yield a
      }
    }
  }

  sealed trait Buffer[Bid, A] {
    val size: Long
    def fdKey(fid: Bid): UIO[FdKey]
    def bytes(a: A): UIO[Bytes]
    def data(b: Bytes): IO[DecodeErr, A]
  }

  def buffer[Bid: MessageCodec, A: MessageCodec](fidPrefix: Bytes, maxSize: Long) =
    new Buffer[Bid, A] {
      val size: Long = maxSize
      def fdKey(fid: Bid): UIO[FdKey] = UIO(FdKey(encodeToBytes(Named(fidPrefix, fid))))
      def bytes(a: A): UIO[Bytes] = UIO(encodeToBytes(a))

      def data(b: Bytes): IO[DecodeErr, A] = Task(decode[A](b)).mapError(DecodeErr(_))
    }

  private object Shard {
    sealed trait Msg
    case class Add(fid: FdKey, size: Long, data: Bytes) extends Msg
    case class Put(fid: FdKey, idx: Long,  data: Bytes) extends Msg
    case class Response(x: Res[Unit])

    def onMessage(implicit dba: Dba.Service): Msg => ZIO[Entity[Unit] with ZEnv, Nothing, Unit] = {
      case msg: Add =>
        for {
          y <- kvs.circular.add(msg.fid, msg.size, msg.data).either
          x <- ZIO.accessM[Entity[Unit]](_.get.replyToSender(Response(y)).orDie)
          z <- ZIO.access [Entity[Unit]](_.get.context.sender())
        } yield x
      case msg: Put => 
        for {
          y <- kvs.circular.put(msg.fid, msg.idx, msg.data).either
          x <- ZIO.accessM[Entity[Unit]](_.get.replyToSender(Response(y)).orDie)
        } yield x
    }
  }

  object Receiver {
    def props(cb: Res[Unit]=>Unit): Props =
      Props(new Receiver(cb))
  }

  class Receiver(cb: Res[Unit]=>Unit) extends Actor with ActorLogging {
    def receive: Receive = {
      case Shard.Response(x) =>
        cb(x)
        context.stop(self)
      case x =>
        log.error(s"bad response=$x")
        context.stop(self)
    }
  }
}
