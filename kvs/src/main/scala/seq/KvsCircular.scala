package kvs.seq

import kvs.{FdKey, Res}
import proto.{MessageCodec, encode, decode}
import zio.*
import zio.stream.{ZStream, Stream}
import zio.akka.cluster.sharding.{Sharding, Entity}
import _root_.akka.actor.{Actor, ActorLogging, Props}

object KvsCircular {
  trait Service {
    def all[Bid, A](fid: Bid)(implicit i: Buffer[Bid, A]): Stream[Err, A]
    def add[Bid, A](fid: Bid, a: A)(implicit i: Buffer[Bid, A]): IO[Err, Unit]
    def put[Bid, A](fid: Bid, idx: Long, a: A)(implicit i: Buffer[Bid, A]): IO[Err, Unit]
    def get[Bid, A](fid: Bid, idx: Long)(implicit i: Buffer[Bid, A]): IO[Err, Option[A]]
  }

  val live: RLayer[ActorSystem with Dba, KvsCircular] = ZLayer.fromEffect {
    for {
      dba <- ZIO.service[Dba.Service]
      as  <- ZIO.service[ActorSystem.Service]
      sh  <- Sharding.start("kvs_circular_write_shard", Shard.onMessage(dba))
    } yield {
      new Service {
        private implicit val dba1: Dba.Service = dba

        def all[Bid, A](fid: Bid)(implicit i: Buffer[Bid, A]): Stream[Err, A] = {
          for {
            fdKey <- Stream.fromEffect(i.fdKey(fid))
            xs    <- kvs.circular.all(fdKey).mapError(KvsErr(_)).mapM(i.data)
          } yield xs
        }

        def add[Bid, A](fid: Bid, a: A)(implicit i: Buffer[Bid, A]): IO[Err, Unit] = {
          for {
            fdKey  <- i.fdKey(fid)
            bytes  <- i.bytes(a)
            _      <- ZIO.effectAsyncM { (callback: IO[Err, Unit] => Unit) =>
                        val receiver = as.actorOf(Receiver.props{
                          case Right(_) => callback(IO.succeed(()))
                          case Left (e) => callback(IO.fail(KvsErr(e)))
                        })
                        sh.send(hex(fdKey.bytes), Shard.Add(fdKey, size=i.size, bytes), receiver)
                      }.mapError(ShardErr(_))
          } yield ()
        }

        def put[Bid, A](fid: Bid, idx: Long, a: A)(implicit i: Buffer[Bid, A]): IO[Err, Unit] =
          for {
            fdKey  <- i.fdKey(fid)
            bytes  <- i.bytes(a)
            _      <- ZIO.effectAsyncM { (callback: IO[Err, Unit] => Unit) =>
                        val receiver = as.actorOf(Receiver.props{
                          case Right(_) => callback(IO.succeed(()))
                          case Left (e) => callback(IO.fail(KvsErr(e)))
                        })
                        sh.send(hex(fdKey.bytes), Shard.Put(fdKey, idx=idx, bytes), receiver)
                      }.mapError(ShardErr(_))
          } yield ()

        def get[Bid, A](fid: Bid, idx: Long)(implicit i: Buffer[Bid, A]): IO[Err, Option[A]] = {
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
  }

  sealed trait Buffer[Bid, A] {
    val size: Long
    def fdKey(fid: Bid): UIO[FdKey]
    def bytes(a: A): UIO[Array[Byte]]
    def data(b: Array[Byte]): IO[DecodeErr, A]
  }

  def buffer[Bid: MessageCodec, A: MessageCodec](fidPrefix: Array[Byte], maxSize: Long) =
    new Buffer[Bid, A] {
      val size: Long = maxSize
      def fdKey(fid: Bid): UIO[FdKey] = UIO(FdKey(encode(Named(fidPrefix, fid))))
      def bytes(a: A): UIO[Array[Byte]] = UIO(encode(a))

      def data(b: Array[Byte]): IO[DecodeErr, A] = Task(decode[A](b)).mapError(DecodeErr(_))
    }

  private object Shard {
    sealed trait Msg
    case class Add(fid: FdKey, size: Long, data: Array[Byte]) extends Msg
    case class Put(fid: FdKey, idx: Long, data: Array[Byte]) extends Msg
    case class Response(x: Res[Unit])

    def onMessage(implicit dba: Dba.Service): Msg => ZIO[Entity[Unit], Nothing, Unit] = {
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
    def props(cb: Res[Unit] => Unit): Props =
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
