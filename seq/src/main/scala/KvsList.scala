package kvs.seq

import kvs.{FdKey, ElKey, EnKey, Res}
import zd.proto.api.{MessageCodec, encodeToBytes, decode}
import zd.proto.Bytes
import zio.{IO, Task, ZLayer, ZIO, UIO}
import zio.akka.cluster.sharding.{Sharding, Entity}
import zio.macros.accessible
import zio.stream.Stream

@accessible
object KvsList {
  trait Service {
    def all[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, after: Option[Key]): Stream[Err, (Key, A)]
    def apply[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, key: Key): IO[Err, A]
    def get[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, key: Key): IO[Err, Option[A]]
    def head[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid): IO[Err, Option[(Key, A)]]
    def prepend[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, a: A): IO[Err, Key]
    def prepend[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, key: Key, a: A): IO[Err, Unit]
    def put[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, key: Key, a: A): IO[Err, Unit]
    def putBulk[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, a: Vector[(Key, A)]): IO[Err, Unit]
    def remove[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A](fid: Fid, key: Key): IO[Err, Boolean]
    def cleanup[Fid: ListFid[*, Key, A]: MessageCodec, Key, A](fid: Fid): IO[Err, Unit]
    def fix[Fid: ListFid[*, Key, A]: MessageCodec, Key, A](fid: Fid): IO[Err, Unit]
  }

  def live: ZLayer[ActorSystem with Dba, Throwable, KvsList] = ZLayer.fromEffect {
    for {
      dba <- ZIO.service[Dba.Service]
      as  <- ZIO.environment[ActorSystem]
      sh  <- Sharding.start("kvs_list_write_shard", Shard.onMessage(dba)).provide(as)
    } yield {
      new Service {
        private implicit val dba1 = dba

        def all[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, after: Option[Key]): Stream[Err, (Key, A)] =
          Stream
            .fromIterableM(
              for {
                fdKey <- UIO(FdKey(encodeToBytes(fid)))
                it    <- ZIO.fromEither(kvs.feed.all(fdKey)).mapError(KvsErr(_))
              } yield it
            )
            .mapM(kvsRes =>
              for {
                res <- ZIO.fromEither(kvsRes).mapError(KvsErr(_))
                key <- Task(decode[Key](res._1.bytes)).mapError(DecodeErr(_))
                a   <- Task(decode[A](res._2)).mapError(DecodeErr(_))
              } yield key -> a
            )

        def apply[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, key: Key): IO[Err, A] =
          for {
            fdKey <- UIO(FdKey(encodeToBytes(fid)))
            elKey <- UIO(ElKey(encodeToBytes(key)))
            res   <- ZIO.fromEither(kvs.feed.apply(EnKey(fdKey, elKey))).mapError(KvsErr(_))
            a     <- Task(decode[A](res)).mapError(DecodeErr(_))
          } yield a

        def get[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, key: Key): IO[Err, Option[A]] =
          for {
            fdKey  <- UIO(FdKey(encodeToBytes(fid)))
            elKey  <- UIO(ElKey(encodeToBytes(key)))
            res    <- ZIO.fromEither(kvs.feed.get(EnKey(fdKey, elKey))).mapError(KvsErr(_))
            a      <- (for {
                        bytes <- ZIO.fromOption(res)
                        a     <- Task(decode[A](bytes)).mapError(e => Some(DecodeErr(e)))
                      } yield a).optional
          } yield a

        def head[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid): IO[Err, Option[(Key, A)]] =
          for {
            fdKey  <- UIO(FdKey(encodeToBytes(fid)))
            res    <- ZIO.fromEither(kvs.feed.head(fdKey)).mapError(KvsErr(_))
            a      <- (for {
                        data <- ZIO.fromOption(res)
                        key  <- Task(decode[Key](data._1.bytes)).mapError(e => Some(DecodeErr(e)))
                        a    <- Task(decode[A](data._2)).mapError(e => Some(DecodeErr(e)))
                      } yield key -> a).optional
          } yield a

        def prepend[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, a: A): IO[Err, Key] =
          for {
            fdKey <- UIO(FdKey(encodeToBytes(fid)))
            eA    <- UIO(encodeToBytes(a))
            res   <- sh.ask[Res[ElKey]](hex(fdKey.bytes), Shard.Add1(fdKey, eA)).mapError(ShardErr(_))
            elKey <- ZIO.fromEither(res).mapError(KvsErr(_))
            key   <- Task(decode[Key](elKey.bytes)).mapError(DecodeErr(_))
          } yield key
        
        def prepend[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, key: Key, a: A): IO[Err, Unit] =
          for {
            fdKey <- UIO(FdKey(encodeToBytes(fid)))
            elKey <- UIO(ElKey(encodeToBytes(key)))
            eA    <- UIO(encodeToBytes(a))
            _     <- sh.ask[Res[Unit]](hex(fdKey.bytes), Shard.Add(fdKey, elKey, eA)).mapError(ShardErr(_))
          } yield ()

        def put[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, key: Key, a: A): IO[Err, Unit] =
          for {
            fdKey <- UIO(FdKey(encodeToBytes(fid)))
            elKey <- UIO(ElKey(encodeToBytes(key)))
            eA    <- UIO(encodeToBytes(a))
            res   <- sh.ask[Res[Unit]](hex(fdKey.bytes), Shard.Put(fdKey, elKey, eA)).mapError(ShardErr(_))
            _     <- ZIO.fromEither(res).mapError(KvsErr(_))
          } yield ()

        def remove[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A](fid: Fid, key: Key): IO[Err, Boolean] =
          for {
            fdKey <- UIO(FdKey(encodeToBytes(fid)))
            elKey <- UIO(ElKey(encodeToBytes(key)))
            shRes <- sh.ask[Res[Boolean]](hex(fdKey.bytes), Shard.Remove(fdKey, elKey)).mapError(ShardErr(_))
            res   <- ZIO.fromEither(shRes).mapError(KvsErr(_))
          } yield res

        def cleanup[Fid: ListFid[*, Key, A]: MessageCodec, Key, A](fid: Fid): IO[Err, Unit] =
          for {
            fdKey <- UIO(FdKey(encodeToBytes(fid)))  
            res   <- sh.ask[Res[Unit]](hex(fdKey.bytes), Shard.Cleanup(fdKey)).mapError(ShardErr(_))
            _     <- ZIO.fromEither(res).mapError(KvsErr(_))
          } yield ()
        
        def fix[Fid: ListFid[*, Key, A]: MessageCodec, Key, A](fid: Fid): IO[Err, Unit] =
          for {
            fdKey <- UIO(FdKey(encodeToBytes(fid)))  
            res   <- sh.ask[Res[Unit]](hex(fdKey.bytes), Shard.Cleanup(fdKey)).mapError(ShardErr(_))
            _     <- ZIO.fromEither(res).mapError(KvsErr(_))
          } yield ()

        def putBulk[Fid: ListFid[*, Key, A]: MessageCodec, Key: MessageCodec, A: MessageCodec](fid: Fid, a: Vector[(Key, A)]): IO[Err, Unit] =
          for {
            fdKey <- UIO(FdKey(encodeToBytes(fid)))  
            data  <- UIO(a.map{ case (key, v) =>  ElKey(encodeToBytes(key)) -> encodeToBytes(v) })
            _     <- sh.ask[Unit](hex(fdKey.bytes), Shard.PutBulk(fdKey, data)).mapError(ShardErr(_))
          } yield ()
      }
    }
  }

  private object Shard {
    sealed trait Msg
    case class Add1(fid: FdKey, data: Bytes) extends Msg
    case class Add(fid: FdKey, id: ElKey, data: Bytes) extends Msg
    case class Put(fid: FdKey, id: ElKey, data: Bytes) extends Msg
    case class Remove(fid: FdKey, id: ElKey) extends Msg
    case class Cleanup(fid: FdKey) extends Msg
    case class Fix(fid: FdKey) extends Msg
    case class PutBulk(fid: FdKey, a: Vector[(ElKey, Bytes)]) extends Msg
    
    def onMessage(implicit dba: Dba.Service): Msg => ZIO[Entity[Unit], Nothing, Unit] = {
      case msg: Add1    => ZIO.accessM[Entity[Unit]](_.get.replyToSender(kvs.feed.prepend(msg.fid, msg.data)).orDie)
      case msg: Add     => ZIO.accessM[Entity[Unit]](_.get.replyToSender(kvs.feed.prepend(EnKey(msg.fid, msg.id), msg.data)).orDie)
      case msg: Put     => ZIO.accessM[Entity[Unit]](_.get.replyToSender(kvs.feed.put(EnKey(msg.fid, msg.id), msg.data)).orDie)
      case msg: Remove  => ZIO.accessM[Entity[Unit]](_.get.replyToSender(kvs.feed.remove(EnKey(msg.fid, msg.id))).orDie)
      case msg: Cleanup => ZIO.accessM[Entity[Unit]](_.get.replyToSender(kvs.feed.cleanup(msg.fid)).orDie)
      case msg: Fix     => ZIO.accessM[Entity[Unit]](_.get.replyToSender(kvs.feed.fix(msg.fid)).orDie)
      case msg: PutBulk =>
        ZIO.foreach_(msg.a){ case (key, v) =>
          Task(kvs.feed.put(EnKey(msg.fid, key), v))
        }.orDie *> ZIO.accessM(_.get.replyToSender(()).orDie)
    }
  }
}
