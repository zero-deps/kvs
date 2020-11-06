package kvs.seq

import kvs.{FdKey, ElKey, EnKey, Res}
import zd.proto.api.{MessageCodec, encodeToBytes, decode}
import zd.proto.Bytes
import zio.{IO, Task, ZLayer, ZIO, UIO}
import zio.stream.Stream
import zio.akka.cluster.sharding.{Sharding, Entity}
import zio.macros.accessible

@accessible
object KvsCircular {
  trait Service {
    def all[Fid, A](fid: Fid)(implicit i: Feed[Fid, A]): Stream[Err, A]
    def add[Fid, A](fid: Fid, a: A)(implicit i: Feed[Fid, A]): IO[Err, Unit]
    def put[Fid, A](fid: Fid, idx: Long, a: A)(implicit i: Feed[Fid, A]): IO[Err, Unit]
    def get[Fid, A](fid: Fid, idx: Long)(implicit i: Feed[Fid, A]): IO[Err, Option[A]]
  }

  def live: ZLayer[ActorSystem with Dba, Throwable, KvsCircular] = ZLayer.fromEffect {
    for {
      dba <- ZIO.service[Dba.Service]
      as  <- ZIO.environment[ActorSystem]
      sh  <- Sharding.start("kvs_circular_write_shard", Shard.onMessage(dba)).provide(as)
    } yield {
      new Service {
        private implicit val dba1 = dba

        def all[Fid, A](fid: Fid)(implicit i: Feed[Fid, A]): Stream[Err, A] =
          Stream
            .fromIterableM(
              for {
                fdKey  <- i.fdKey(fid)
                stream <- IO.fromEither(kvs.circular.all(fdKey)).mapError(KvsErr(_))
              } yield stream
            )
            .mapM(kvsRes =>
              for {
                bytes <- IO.fromEither(kvsRes).mapError(KvsErr(_))
                a     <- i.data(bytes)
              } yield a
            )

        def add[Fid, A](fid: Fid, a: A)(implicit i: Feed[Fid, A]): IO[Err, Unit] =
          for {
            fdKey <- i.fdKey(fid)
            bytes <- i.bytes(a)
            res   <- sh.ask[Res[Unit]](hex(fdKey.bytes), Shard.Add(fdKey, size=i.size, bytes)).mapError(ShardErr(_))
            _     <- IO.fromEither(res).mapError(KvsErr(_))
          } yield ()

        def put[Fid, A](fid: Fid, idx: Long, a: A)(implicit i: Feed[Fid, A]): IO[Err, Unit] =
          for {
            fdKey <- i.fdKey(fid)
            bytes <- i.bytes(a)
            res   <- sh.ask[Res[Unit]](hex(fdKey.bytes), Shard.Put(fdKey, idx=idx, bytes)).mapError(ShardErr(_))
            _     <- IO.fromEither(res).mapError(KvsErr(_))
          } yield ()

        def get[Fid, A](fid: Fid, idx: Long)(implicit i: Feed[Fid, A]): IO[Err, Option[A]] =
          for {
            fdKey  <- i.fdKey(fid)
            res    <- IO.fromEither(kvs.circular.get(fdKey)(idx)).mapError(KvsErr(_))
            a      <- (for {
                        bytes <- ZIO.fromOption(res)
                        a     <- i.data(bytes).mapError(Some(_))
                      } yield a).optional
          } yield a
      }
    }
  }

  sealed trait Feed[Fid, A] {
    val size: Long
    def fdKey(fid: Fid): UIO[FdKey]
    def bytes(a: A): UIO[Bytes]

    def data(b: Bytes): IO[DecodeErr, A]
  }

  def feed[Fid: MessageCodec, A: MessageCodec](fidPrefix: String, maxSize: Long) =
    new Feed[Fid, A] {
      val size: Long = maxSize
      def fdKey(fid: Fid): UIO[FdKey] = UIO(FdKey(encodeToBytes(Named(fidPrefix, fid))))
      def bytes(a: A): UIO[Bytes] = UIO(encodeToBytes(a))

      def data(b: Bytes): IO[DecodeErr, A] = Task(decode[A](b)).mapError(DecodeErr(_))
    }

  private object Shard {
    sealed trait Msg
    final case class Add(fid: FdKey, size: Long, data: Bytes) extends Msg
    final case class Put(fid: FdKey, idx: Long, data: Bytes) extends Msg

    def onMessage(implicit dba: Dba.Service): Msg => ZIO[Entity[Unit], Nothing, Unit] = {
      case msg: Add => ZIO.accessM[Entity[Unit]](_.get.replyToSender(kvs.circular.add(msg.fid, msg.size, msg.data)).orDie)
      case msg: Put => ZIO.accessM[Entity[Unit]](_.get.replyToSender(kvs.circular.put(msg.fid, msg.idx, msg.data)).orDie)
    }
  }
}
