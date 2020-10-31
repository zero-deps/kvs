package kvs.seq

import kvs.{FdKey, ElKey, EnKey, Res}
import zd.proto.api.{MessageCodec, encodeToBytes, decode}
import zd.proto.Bytes
import zio.{IO, Task, ZLayer, ZIO, UIO}
import zio.stream.Stream
import zio.akka.cluster.sharding.{Sharding, Entity}
import zio.macros.accessible

@accessible
object KvsArray {
  trait Service {
    def all[Fid: ArrayFid[*, A]: MessageCodec, A: MessageCodec](fid: Fid): Stream[Err, A]
    def add[Fid: ArrayFid[*, A]: MessageCodec, A: MessageCodec](fid: Fid, a: A): IO[Err, Unit]
    def put[Fid: ArrayFid[*, A]: MessageCodec, A: MessageCodec](fid: Fid, idx: Long, a: A): IO[Err, Unit]
    def get[Fid: ArrayFid[*, A]: MessageCodec, A: MessageCodec](fid: Fid, idx: Long): IO[Err, Option[A]]
  }
  
  def live: ZLayer[ActorSystem with Dba, Throwable, KvsArray] = ZLayer.fromEffect {
    for {
      dba <- ZIO.service[Dba.Service]
      as  <- ZIO.environment[ActorSystem]
      sh  <- Sharding.start("kvs_array_write_shard", Shard.onMessage(dba)).provide(as)
    } yield {
      new Service {
        private implicit val dba1 = dba

        def all[Fid: ArrayFid[*, A]: MessageCodec, A: MessageCodec](fid: Fid): Stream[Err, A] =
          Stream
            .fromIterableM(
              for {
                eFid   <- UIO(encodeToBytes(fid))
                stream <- IO.fromEither(kvs.array.all(FdKey(eFid))).mapError(KvsErr(_))
              } yield stream
            )
            .mapM(kvsRes =>
              for {
                bytes <- IO.fromEither(kvsRes).mapError(KvsErr(_))
                a     <- IO.effect(decode[A](bytes)).mapError(DecodeErr(_))
              } yield a
            )

        def add[Fid: ArrayFid[*, A]: MessageCodec, A: MessageCodec](fid: Fid, a: A): IO[Err, Unit] =
          for {
            eFid <- UIO(encodeToBytes(fid))
            eA   <- UIO(encodeToBytes(a))
            i    <- UIO(implicitly[ArrayFid[Fid, A]])
            res  <- sh.ask[Res[Unit]](hex(eFid), Shard.Add(FdKey(eFid), size=i.size, eA)).mapError(ShardErr(_))
            _    <- IO.fromEither(res).mapError(KvsErr(_))
          } yield ()

        def put[Fid: ArrayFid[*, A]: MessageCodec, A: MessageCodec](fid: Fid, idx: Long, a: A): IO[Err, Unit] =
          for {
            eFid <- UIO(encodeToBytes(fid))
            eA   <- UIO(encodeToBytes(a))
            res  <- sh.ask[Res[Unit]](hex(eFid), Shard.Put(FdKey(eFid), idx=idx, eA)).mapError(ShardErr(_))
            _    <- IO.fromEither(res).mapError(KvsErr(_))
          } yield ()
        
        def get[Fid: ArrayFid[*, A]: MessageCodec, A: MessageCodec](fid: Fid, idx: Long): IO[Err, Option[A]] =
          for {
            eFid <- UIO(encodeToBytes(fid))
            res  <- IO.fromEither(kvs.array.get(FdKey(eFid))(idx)).mapError(KvsErr(_))
            a    <- (for {
                      bytes <- ZIO.fromOption(res)
                      a     <- IO.effect(decode[A](bytes)).mapError(e => Some(DecodeErr(e)))
                    } yield a).optional
          } yield a
      }
    }
  }

  private object Shard {
    sealed trait Msg
    final case class Add(fid: FdKey, size: Long, data: Bytes) extends Msg
    final case class Put(fid: FdKey, idx: Long, data: Bytes) extends Msg

    def onMessage(implicit dba: Dba.Service): Msg => ZIO[Entity[Unit], Nothing, Unit] = {
      case msg: Add => ZIO.accessM[Entity[Unit]](_.get.replyToSender(kvs.array.add(msg.fid, msg.size, msg.data)).orDie)
      case msg: Put => ZIO.accessM[Entity[Unit]](_.get.replyToSender(kvs.array.put(msg.fid, msg.idx, msg.data)).orDie)
    }
  }
}