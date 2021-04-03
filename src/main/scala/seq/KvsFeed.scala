package kvs.seq

import zero.ext._, either._
import proto.{MessageCodec, encodeToBytes, decode}
import proto.Bytes
import zio._
import zio.akka.cluster.sharding.{Sharding, Entity}
import zio.macros.accessible
import zio.stream.{ZStream, Stream}
import _root_.akka.actor.{Actor, ActorLogging, Props}
import kvs.{Fd, FdKey, ElKey, EnKey, Res}

@accessible
object KvsFeed {
  trait Service {
    def all  [Fid, Key, A](fid: Fid              )(implicit i: AnyFeed[Fid, Key, A]): Stream[Err, (Key, A)]
    def all  [Fid, Key, A](fid: Fid, start: ElKey)(implicit i: AnyFeed[Fid, Key, A]): Stream[Err, (Key, A)]
    def apply[Fid, Key, A](fid: Fid, key: Key    )(implicit i: AnyFeed[Fid, Key, A]):     IO[Err, A]
    def get  [Fid, Key, A](fid: Fid, key: Key    )(implicit i: AnyFeed[Fid, Key, A]):     IO[Err, Option[A]]
    def head [Fid, Key, A](fid: Fid              )(implicit i: AnyFeed[Fid, Key, A]):     IO[Err, Option[(Key, A)]]

    def add[Fid, Key, A](fid: Fid, a: A)(implicit i: Increment[Fid, Key, A]): IO[Err, Key]

    def put    [Fid, Key, A](fid: Fid, key: Key, a: A     )(implicit i: Manual[Fid, Key, A]): IO[Err, Unit]
    def putBulk[Fid, Key, A](fid: Fid, a: Vector[(Key, A)])(implicit i: Manual[Fid, Key, A]): IO[Err, Unit]

    def remove [Fid, Key, A](fid: Fid, key: Key)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Boolean]
    def cleanup[Fid, Key, A](fid: Fid          )(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Unit]
    def fix    [Fid, Key, A](fid: Fid, fd: Fd  )(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Unit]
  }

  val live: RLayer[ActorSystem with Dba, KvsFeed] = ZLayer.fromEffect {
    for {
      dba <- ZIO.service[Dba.Service]
      as  <- ZIO.service[ActorSystem.Service]
      sh  <- Sharding.start("kvs_list_write_shard", Shard.onMessage(dba))
    } yield {
      new Service {
        private implicit val dba1 = dba

        def all[Fid, Key, A](fid: Fid)(implicit i: AnyFeed[Fid, Key, A]): Stream[Err, (Key, A)] = {
          for {
            fdKey  <- Stream.fromEffect(i.fdKey(fid))
            xs     <- kvs.feed.all(fdKey).mapError(KvsErr(_)).mapM{ case (k,a) =>
                        for {
                          k2 <- i.key(k)
                          a2 <- i.data(a)
                        } yield k2->a2
                      }
          } yield xs
        }

        def all[Fid, Key, A](fid: Fid, start: ElKey)(implicit i: AnyFeed[Fid, Key, A]): Stream[Err, (Key, A)] = {
          for {
            fdKey  <- Stream.fromEffect(i.fdKey(fid))
            xs     <- kvs.feed.all(fdKey, start).mapError(KvsErr(_)).mapM{ case (k,a) =>
                        for {
                          k2 <- i.key(k)
                          a2 <- i.data(a)
                        } yield k2->a2
                      }
          } yield xs
        }

        def apply[Fid, Key, A](fid: Fid, key: Key)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, A] = {
          for {
            fdKey <- i.fdKey(fid)
            elKey <- i.elKey(key)
            bytes <- kvs.feed.apply(EnKey(fdKey, elKey)).mapError(KvsErr(_))
            a     <- i.data(bytes)
          } yield a
        }

        def get[Fid, Key, A](fid: Fid, key: Key)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Option[A]] = {
          for {
            fdKey  <- i.fdKey(fid)
            elKey  <- i.elKey(key)
            res    <- kvs.feed.get(EnKey(fdKey, elKey)).mapError(KvsErr(_))
            a      <- (for {
                        bytes <- ZIO.fromOption(res)
                        a     <- i.data(bytes).mapError(Some(_))
                      } yield a).optional
          } yield a
        }

        def head[Fid, Key, A](fid: Fid)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Option[(Key, A)]] = {
          for {
            fdKey  <- i.fdKey(fid)
            kvsRes <- kvs.feed.head(fdKey).mapError(KvsErr(_))
            a      <- (for {
                        res <- ZIO.fromOption(kvsRes)
                        key <- i.key(res._1).mapError(Some(_))
                        a   <- i.data(res._2).mapError(Some(_))
                      } yield key -> a).optional
          } yield a
        }

        def add[Fid, Key, A](fid: Fid, a: A)(implicit i: Increment[Fid, Key, A]): IO[Err, Key] =
          for {
            fdKey  <- i.fdKey(fid)
            bytes  <- i.bytes(a)
            elKey  <- ZIO.effectAsyncM { (callback: IO[Err, ElKey] => Unit) =>
                        val receiver = as.actorOf(Props(new Actor with ActorLogging {
                          def receive: Receive = {
                            case Shard.Added(Right(a)) =>
                              callback(IO.succeed(a))
                              context.stop(self)
                            case Shard.Added(Left(e)) =>
                              callback(IO.fail(KvsErr(e)))
                              context.stop(self)
                            case x =>
                              log.error(s"bad response=$x")
                              context.stop(self)
                          }
                        }))
                        sh.send(hex(fdKey.bytes), Shard.Add(fdKey, bytes), receiver)
                      }.mapError(ShardErr(_))
            key    <- i.key(elKey)
          } yield key

        def put[Fid, Key, A](fid: Fid, key: Key, a: A)(implicit i: Manual[Fid, Key, A]): IO[Err, Unit] =
          for {
            fdKey  <- i.fdKey(fid)
            elKey  <- i.elKey(key)
            bytes  <- i.bytes(a)
            _      <- ZIO.effectAsyncM { (callback: IO[Err, Unit] => Unit) =>
                        val receiver = as.actorOf(Receiver.props{
                          case Right(a) => callback(IO.succeed(a))
                          case Left (e) => callback(IO.fail(KvsErr(e)))
                        })
                        sh.send(hex(fdKey.bytes), Shard.Put(fdKey, elKey, bytes), receiver)
                      }.mapError(ShardErr(_))
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
            _      <- ZIO.effectAsyncM { (callback: IO[Err, Unit] => Unit) =>
                        val receiver = as.actorOf(Receiver.props{
                          case Right(a) => callback(IO.succeed(a))
                          case Left (e) => callback(IO.fail(KvsErr(e)))
                        })
                        sh.send(hex(fdKey.bytes), Shard.PutBulk(fdKey, data), receiver)
                      }.mapError(ShardErr(_))
          } yield ()

        def remove[Fid, Key, A](fid: Fid, key: Key)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Boolean] =
          for {
            fdKey  <- i.fdKey(fid)
            elKey  <- i.elKey(key)
            res    <- ZIO.effectAsyncM { (callback: IO[Err, Boolean] => Unit) =>
                        val receiver = as.actorOf(Props(new Actor with ActorLogging {
                          def receive: Receive = {
                            case Shard.Removed(Right(a)) =>
                              callback(IO.succeed(a))
                              context.stop(self)
                            case Shard.Removed(Left(e)) =>
                              callback(IO.fail(KvsErr(e)))
                              context.stop(self)
                            case x =>
                              log.error(s"bad response=$x")
                              context.stop(self)
                          }
                        }))
                        sh.send(hex(fdKey.bytes), Shard.Remove(fdKey, elKey), receiver)
                      }.mapError(ShardErr(_))
          } yield res

        def cleanup[Fid, Key, A](fid: Fid)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Unit] =
          for {
            fdKey  <- i.fdKey(fid)
            _      <- ZIO.effectAsyncM { (callback: IO[Err, Unit] => Unit) =>
                        val receiver = as.actorOf(Receiver.props{
                          case Right(a) => callback(IO.succeed(a))
                          case Left (e) => callback(IO.fail(KvsErr(e)))
                        })
                        sh.send(hex(fdKey.bytes), Shard.Cleanup(fdKey), receiver)
                      }.mapError(ShardErr(_))
          } yield ()

        def fix[Fid, Key, A](fid: Fid, fd: Fd)(implicit i: AnyFeed[Fid, Key, A]): IO[Err, Unit] =
          for {
            fdKey  <- i.fdKey(fid)
            _      <- ZIO.effectAsyncM { (callback: IO[Err, Unit] => Unit) =>
                        val receiver = as.actorOf(Receiver.props{
                          case Right(a) => callback(IO.succeed(a))
                          case Left (e) => callback(IO.fail(KvsErr(e)))
                        })
                        sh.send(hex(fdKey.bytes), Shard.Fix(fdKey, fd), receiver)
                      }.mapError(ShardErr(_))
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

  def manualFeed[Fid: MessageCodec, Key: MessageCodec, A: MessageCodec](fidPrefix: Bytes): Manual[Fid, Key, A] =
    new Manual[Fid, Key, A] {
      def fdKey(fid: Fid): UIO[FdKey] = UIO(FdKey(encodeToBytes(Named(fidPrefix, fid))))
      def elKey(key: Key): UIO[ElKey] = UIO(ElKey(encodeToBytes(key)))
      def bytes(a: A): UIO[Bytes] = UIO(encodeToBytes(a))

      def key(elKey: ElKey): IO[DecodeErr, Key] = Task(decode[Key](elKey.bytes)).mapError(DecodeErr(_))
      def data(b: Bytes): IO[DecodeErr, A] = Task(decode[A](b)).mapError(DecodeErr(_))
    }

  def incrementFeed[Fid: MessageCodec, Key, A: MessageCodec](fidPrefix: Bytes, from: Key => Bytes, to: Bytes => Key): Increment[Fid, Key, A] =
    new Increment[Fid, Key, A] {
      def fdKey(fid: Fid): UIO[FdKey] = UIO(FdKey(encodeToBytes(Named(fidPrefix, fid))))
      def elKey(key: Key): UIO[ElKey] = UIO(ElKey(from(key)))
      def bytes(a: A): UIO[Bytes] = UIO(encodeToBytes(a))

      def key(elKey: ElKey): IO[DecodeErr, Key] = UIO(to(elKey.bytes))
      def data(b: Bytes): IO[DecodeErr, A] = Task(decode[A](b)).mapError(DecodeErr(_))
    }

  private object Shard {
    sealed trait Msg
    case class Add    (fid: FdKey,            data: Bytes  ) extends Msg
    case class Put    (fid: FdKey, id: ElKey, data: Bytes  ) extends Msg
    case class Remove (fid: FdKey, id: ElKey               ) extends Msg
    case class Cleanup(fid: FdKey                          ) extends Msg
    case class Fix    (fid: FdKey, fd: Fd                  ) extends Msg
    case class PutBulk(fid: FdKey, a: Vector[(ElKey,Bytes)]) extends Msg
    case class Response(x: Res[Unit])
    case class Added   (x: Res[ElKey])
    case class Removed (x: Res[Boolean])

    def onMessage(implicit dba: Dba.Service): Msg => ZIO[Entity[Unit], Nothing, Unit] = {
      case msg: Add =>
        for {
          y <- kvs.feed.add(msg.fid, msg.data).either
          x <- ZIO.accessM[Entity[Unit]](_.get.replyToSender(Added(y)).orDie)
        } yield x
      case msg: Put =>
        for {
          y <- kvs.feed.put(EnKey(msg.fid, msg.id), msg.data).either
          x <- ZIO.accessM[Entity[Unit]](_.get.replyToSender(Response(y)).orDie)
        } yield x
      case msg: Remove =>
        for {
          y <- kvs.feed.remove(EnKey(msg.fid, msg.id)).either
          x <- ZIO.accessM[Entity[Unit]](_.get.replyToSender(Removed(y)).orDie)
        } yield x
      case msg: Cleanup =>
        for {
          y <- kvs.feed.cleanup(msg.fid).either
          x <- ZIO.accessM[Entity[Unit]](_.get.replyToSender(Response(y)).orDie)
        } yield x
      case msg: Fix =>
        for {
          y <- kvs.feed.fix(msg.fid, msg.fd).either
          x <- ZIO.accessM[Entity[Unit]](_.get.replyToSender(Response(y)).orDie)
        } yield x
      case msg: PutBulk =>
        ZIO.foreach_(msg.a){ case (key, v) =>
          Task(kvs.feed.put(EnKey(msg.fid, key), v))
        }.orDie *> ZIO.accessM(_.get.replyToSender(Response(().right)).orDie)
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
