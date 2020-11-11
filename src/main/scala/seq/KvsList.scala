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
    def all[Fid, Key, A](fid: Fid, after: Option[Key])(implicit i: AnyFeed[Fid, Key, A]): Stream[Err, (Key, A)]
    def apply[Fid, Key, A](fid: Fid, key: Key)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, A]
    def get[Fid, Key, A](fid: Fid, key: Key)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Option[A]]
    def head[Fid, Key, A](fid: Fid)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Option[(Key, A)]]

    def prepend[Fid, Key, A](fid: Fid, a: A)(implicit i: Increment[Fid, Key, A]): IO[Err, Key]

    def prepend[Fid, Key, A](fid: Fid, key: Key, a: A)(implicit i: Manual[Fid, Key, A]): IO[Err, Unit]
    def put[Fid, Key, A](fid: Fid, key: Key, a: A)(implicit i: Manual[Fid, Key, A]): IO[Err, Unit]
    def putBulk[Fid, Key, A](fid: Fid, a: Vector[(Key, A)])(implicit i: Manual[Fid, Key, A]): IO[Err, Unit]

    def remove[Fid, Key, A](fid: Fid, key: Key)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Boolean]
    def cleanup[Fid, Key, A](fid: Fid)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Unit]
    def fix[Fid, Key, A](fid: Fid)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Unit]
  }

  val live: ZLayer[ActorSystem with Dba, Throwable, KvsList] = ZLayer.fromEffect {
    for {
      dba <- ZIO.service[Dba.Service]
      as  <- ZIO.environment[ActorSystem]
      sh  <- Sharding.start("kvs_list_write_shard", Shard.onMessage(dba)).provide(as)
    } yield {
      new Service {
        private implicit val dba1 = dba

        def all[Fid, Key, A](fid: Fid, after: Option[Key])(implicit i: AnyFeed[Fid, Key, A]): Stream[Err, (Key, A)] =
          Stream
            .fromIterableM(
              for {
                fdKey <- i.fdKey(fid)
                it    <- ZIO.fromEither(kvs.feed.all(fdKey)).mapError(KvsErr(_))
              } yield it
            )
            .mapM(kvsRes =>
              for {
                res <- ZIO.fromEither(kvsRes).mapError(KvsErr(_))
                key <- i.key(res._1)
                a   <- i.data(res._2)
              } yield key -> a
            )

        def apply[Fid, Key, A](fid: Fid, key: Key)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, A] =
          for {
            fdKey <- i.fdKey(fid)
            elKey <- i.elKey(key)
            bytes <- ZIO.fromEither(kvs.feed.apply(EnKey(fdKey, elKey))).mapError(KvsErr(_))
            a     <- i.data(bytes)
          } yield a

        def get[Fid, Key, A](fid: Fid, key: Key)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Option[A]] =
          for {
            fdKey  <- i.fdKey(fid)
            elKey  <- i.elKey(key)
            res    <- ZIO.fromEither(kvs.feed.get(EnKey(fdKey, elKey))).mapError(KvsErr(_))
            a      <- (for {
                        bytes <- ZIO.fromOption(res)
                        a     <- i.data(bytes).mapError(Some(_))
                      } yield a).optional
          } yield a

        def head[Fid, Key, A](fid: Fid)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Option[(Key, A)]] =
          for {
            fdKey  <- i.fdKey(fid)
            kvsRes <- ZIO.fromEither(kvs.feed.head(fdKey)).mapError(KvsErr(_))
            a      <- (for {
                        res <- ZIO.fromOption(kvsRes)
                        key <- i.key(res._1).mapError(Some(_))
                        a   <- i.data(res._2).mapError(Some(_))
                      } yield key -> a).optional
          } yield a


        def prepend[Fid, Key, A](fid: Fid, a: A)(implicit i: Increment[Fid, Key, A]): IO[Err, Key] =
          for {
            fdKey <- i.fdKey(fid)
            bytes <- i.bytes(a)
            res   <- sh.ask[Res[ElKey]](hex(fdKey.bytes), Shard.Add1(fdKey, bytes)).mapError(ShardErr(_))
            elKey <- ZIO.fromEither(res).mapError(KvsErr(_))
            key   <- i.key(elKey)
          } yield key


        def prepend[Fid, Key, A](fid: Fid, key: Key, a: A)(implicit i: Manual[Fid, Key, A]): IO[Err, Unit] =
          for {
            fdKey <- i.fdKey(fid)
            elKey <- i.elKey(key)
            bytes <- i.bytes(a)
            _     <- sh.ask[Res[Unit]](hex(fdKey.bytes), Shard.Add(fdKey, elKey, bytes)).mapError(ShardErr(_))
          } yield ()

        def put[Fid, Key, A](fid: Fid, key: Key, a: A)(implicit i: Manual[Fid, Key, A]): IO[Err, Unit] =
          for {
            fdKey <- i.fdKey(fid)
            elKey <- i.elKey(key)
            bytes <- i.bytes(a)
            res   <- sh.ask[Res[Unit]](hex(fdKey.bytes), Shard.Put(fdKey, elKey, bytes)).mapError(ShardErr(_))
            _     <- ZIO.fromEither(res).mapError(KvsErr(_))
          } yield ()

       def putBulk[Fid, Key, A](fid: Fid, elements: Vector[(Key, A)])(implicit i: Manual[Fid, Key, A]): IO[Err, Unit] =
          for {
            fdKey  <- i.fdKey(fid)
            data   <- ZIO.foreach(elements){ case (key, a) =>
                        for {
                          elKey <- i.elKey(key)
                          bytes <- i.bytes(a)
                        } yield elKey -> bytes
                      }
            _      <- sh.ask[Unit](hex(fdKey.bytes), Shard.PutBulk(fdKey, data)).mapError(ShardErr(_))
          } yield ()


        def remove[Fid, Key, A](fid: Fid, key: Key)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Boolean] =
          for {
            fdKey <- i.fdKey(fid)
            elKey <- i.elKey(key)
            shRes <- sh.ask[Res[Boolean]](hex(fdKey.bytes), Shard.Remove(fdKey, elKey)).mapError(ShardErr(_))
            res   <- ZIO.fromEither(shRes).mapError(KvsErr(_))
          } yield res

        def cleanup[Fid, Key, A](fid: Fid)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Unit] =
          for {
            fdKey <- i.fdKey(fid)
            res   <- sh.ask[Res[Unit]](hex(fdKey.bytes), Shard.Cleanup(fdKey)).mapError(ShardErr(_))
            _     <- ZIO.fromEither(res).mapError(KvsErr(_))
          } yield ()

        def fix[Fid, Key, A](fid: Fid)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Unit] =
          for {
            fdKey <- i.fdKey(fid)
            res   <- sh.ask[Res[Unit]](hex(fdKey.bytes), Shard.Cleanup(fdKey)).mapError(ShardErr(_))
            _     <- ZIO.fromEither(res).mapError(KvsErr(_))
          } yield ()
      }
    }
  }

  sealed trait Feed[Fid, Key, A, Flag] {
    def fdKey(fid: Fid): UIO[FdKey]
    def elKey(key: Key): UIO[ElKey]
    def bytes(a: A): UIO[Bytes]

    def key(elKey: ElKey): IO[DecodeErr, Key]
    def data(b: Bytes): IO[DecodeErr, A]
  }
  sealed trait ManualFlag
  sealed trait IncrementFlag

  type AnyFeed[Fid, Key, A] = Feed[Fid, Key, A, _]
  type Manual[Fid, Key, A] = Feed[Fid, Key, A, ManualFlag]
  type Increment[Fid, Key, A] = Feed[Fid, Key, A, IncrementFlag]

  def manualFeed[Fid: MessageCodec, Key: MessageCodec, A: MessageCodec](fidPrefix: String): Manual[Fid, Key, A] =
    new Manual[Fid, Key, A] {
      def fdKey(fid: Fid): UIO[FdKey] = UIO(FdKey(encodeToBytes(Named(fidPrefix, fid))))
      def elKey(key: Key): UIO[ElKey] = UIO(ElKey(encodeToBytes(key)))
      def bytes(a: A): UIO[Bytes] = UIO(encodeToBytes(a))

      def key(elKey: ElKey): IO[DecodeErr, Key] = Task(decode[Key](elKey.bytes)).mapError(DecodeErr(_))
      def data(b: Bytes): IO[DecodeErr, A] = Task(decode[A](b)).mapError(DecodeErr(_))
    }

  def incrementFeed[Fid: MessageCodec, Key, A: MessageCodec](fidPrefix: String, from: Key => Bytes, to: Bytes => Key): Increment[Fid, Key, A] =
    new Increment[Fid, Key, A] {
      def fdKey(fid: Fid): UIO[FdKey] = UIO(FdKey(encodeToBytes(Named(fidPrefix, fid))))
      def elKey(key: Key): UIO[ElKey] = UIO(ElKey(from(key)))
      def bytes(a: A): UIO[Bytes] = UIO(encodeToBytes(a))

      def key(elKey: ElKey): IO[DecodeErr, Key] = UIO(to(elKey.bytes))
      def data(b: Bytes): IO[DecodeErr, A] = Task(decode[A](b)).mapError(DecodeErr(_))
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
