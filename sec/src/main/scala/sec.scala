package zero.kvs

import zio._
// import encoding._
// import zio._, stream._, blocking.{blocking => succeedb, _}, clock._
import zio.akka.cluster.sharding.{Sharding, Entity}
import zd.proto._
// import zd.kvs.{ElKey, EnKey, FdKey}
// import zd.kvs.en.En
import com.typesafe.config.Config
import zd.kvs._

sealed trait ShardMsg
final case class ShardAdd(fid: FdKey, id: ElKey, data: Bytes) extends ShardMsg
final case class ShardPut(fid: FdKey, id: ElKey, data: Bytes) extends ShardMsg
final case class ShardPutBulk(fid: FdKey, a: Vector[(Bytes, Bytes)]) extends ShardMsg
final case class ShardRemove(fid: FdKey, id: ElKey) extends ShardMsg

package object sec {

  // sealed trait KvsErr
  // object KvsErr {
  //     case class IO(e: Throwable) extends RuntimeException(e) with KvsErr
  //     case class Sharding(e: Throwable) extends RuntimeException(e) with KvsErr
  //     case class Decode(e: Throwable) extends RuntimeException(e) with KvsErr

  //     import zd.kvs.{EnKey, PathKey}
  //     case class EntryExists(key: EnKey) extends KvsErr
  //     case class FileNotExists(path: PathKey) extends KvsErr
  //     case class FileAlreadyExists(path: PathKey) extends KvsErr
  //     case class Fail(r: String) extends KvsErr
  //     case class Throwed(e: Throwable) extends RuntimeException(e) with KvsErr
  //     case class AckQuorumFailed(why: String) extends KvsErr
  //     case class AckTimeoutFailed(op: String, k: Bytes) extends KvsErr
  // }

  type ActorSystem = Has[_root_.akka.actor.ActorSystem]
  type Kvs = Has[Kvs.Service]
  type KIO[A] = IO[Err, A]
  type KZIO[R, A] = ZIO[R, Err, A]

  def actorSystem(name: String): TaskLayer[ActorSystem] = ZLayer.fromManaged{
    ZIO.effect(_root_.akka.actor.ActorSystem(name)).toManaged(as => Task.fromFuture(_ => as.terminate()).either)
  }
  def actorSystem(name: String, config: Config): TaskLayer[ActorSystem] = ZLayer.fromManaged{
    ZIO.effect(_root_.akka.actor.ActorSystem(name, config)).toManaged(as => Task.fromFuture(_ => as.terminate()).either)
  }

  // type Codec[A] = MessageCodec[A]

  // def encode[A: Codec](a: A): Array[Byte] = encode(a)
  // def decode[A: Codec](b: Array[Byte]): IO[DecodeErr, A] = IO.effect(decode(b))

  implicit object BytesDataCodec extends DataCodec[Bytes] {
    def extract(x: Bytes): Bytes = x
    def insert(x: Bytes): Bytes = x
  }

  object Kvs {
    trait Service {
  //     def get[A: Codec](fid: String, id: String): IO[KvsErr, Option[A]]
      def put[A: DataCodec](fid: FdKey, id: ElKey, a: A): KIO[A]
  //     def put[A: Codec](fid: String, id: String, a: A): IO[KvsErr, A]
  //     def putBulk[A: Codec](fid: String, a: Vector[(String, A)]): ZIO[Blocking with Clock, KvsErr, Unit]
  //     def remove[A: Codec](fid: String, id: String): IO[KvsErr, Unit]
  //     def stream[A: Codec](fid: String, from: Option[String]): Stream[KvsErr, A]
  //     def cleanup(fid: String): IO[KvsErr, Unit]
    }

  //   def get[A: Codec](fid: String, id: String): ZIO[Kvs, KvsErr, Option[A]] = ZIO.accessM(_.get.get(fid, id))
    def put[A: DataCodec](fid: FdKey, id: ElKey, a: A): KZIO[Kvs, A] = ZIO.accessM(_.get.put(fid, id, a))
  //   def put[A: Codec](fid: String, id: String, a: A): ZIO[Kvs, KvsErr, A] = ZIO.accessM(_.get.put(fid, id, a))
  //   def putBulk[A: Codec](fid: String, a: Vector[(String, A)]): ZIO[Kvs with Blocking with Clock, KvsErr, Unit] = ZIO.accessM(_.get.putBulk(fid, a))
  //   def remove[A: Codec](fid: String, id: String): ZIO[Kvs, KvsErr, Unit] = ZIO.accessM(_.get.remove(fid, id))
  //   def stream[A: Codec](fid: String, from: Option[String] = None): ZIO[Kvs, Nothing, Stream[KvsErr, A]] = ZIO.access(_.get.stream(fid, from))
  //   def cleanup(fid: String): ZIO[Kvs, Nothing, Unit] = ZIO.access(x => { x.get.cleanup(fid); () })

    val live: ZLayer[ActorSystem, Err, Kvs] = ZLayer.fromEffect{
      for {
        as   <- ZIO.access[ActorSystem](_.get)
        kvs  <- ZIO.effect(zd.kvs.Kvs(as))
        sh   <- Sharding.start("write_shard", onMessage=writeShard(kvs)).provideLayer(ZLayer.succeed(as))
        res  <- ZIO.succeed(new Service {
  //                 def get[A: Codec](fid: String, id: String): IO[KvsErr, Option[A]] = for {
  //                   res  <- IO.fromEither(kvs.get(EnKey(FdKey(bytes(fid)), bytes(id)))).mapError(kvserr)
  //                   a    <- extractOpt[A](res)
  //                 } yield a

  //                 def add[A: Codec](fid: String, id: String, a: A): IO[KvsErr, A] =
  //                   sh.send(fid, ShardAdd(bytes(fid), bytes(id), bytes(a))).as(a)
  //                     .mapError(KvsErr.Sharding(_):KvsErr)
                  
                  def put[A: DataCodec](fid: FdKey, id: ElKey, a: A): KIO[A] = ???
                    // sh.send(fid, ShardPut(fid, id, bytes(a))).as(a)
                    //   .mapError(KvsErr.Sharding(_):KvsErr)

  //                 def putBulk[A: Codec](fid: String, a: Vector[(String, A)]): ZIO[Blocking with Clock, KvsErr, Unit] = for {
  //                   b  <- succeedb(UIO.succeed(a.map(kv => (bytes(kv._1), bytes(kv._2))))).timed
  //                   _  <- UIO.succeed(println(s"enc=${b._1.toMillis}"))
  //                   r  <- sh.ask[Unit](fid, ShardPutBulk(bytes(fid), b._2)).mapError(KvsErr.Sharding(_):KvsErr).timed
  //                   _  <- UIO.succeed(println(s"put=${r._1.toMillis}"))
  //                 } yield ()

  //                 def remove[A: Codec](fid: String, id: String): IO[KvsErr, Unit] =
  //                   sh.send(fid, ShardRemove(bytes(fid), bytes(id)))
  //                     .mapError(KvsErr.Sharding(_):KvsErr)

  //                 def stream[A: Codec](fid: String, from: Option[String]): Stream[KvsErr, A] = 
  //                   Stream
  //                     .fromIterableM(IO.fromEither(kvs.all(FdKey(bytes(fid)), from.map(id => Some(bytes(id))))).mapError(kvserr))
  //                     .mapM(IO.fromEither(_).mapError(kvserr))
  //                     .mapM(b => extract[A](b.en))
                    
  //                 def cleanup(fid: String): IO[KvsErr, Unit] = IO.fromEither(kvs.cleanup(FdKey(bytes(fid)))).mapError(kvserr)
                })
      } yield res
    }.mapError(Throwed(_))
  }

  def writeShard(kvs: zd.kvs.Kvs): ShardMsg => ZIO[Entity[Unit], Nothing, Unit] = {
    case msg: ShardAdd => ZIO.effect(kvs.add(msg.fid, msg.id, msg.data)).ignore
    case msg: ShardPut => ZIO.effect(kvs.put(msg.fid, msg.id, msg.data)).ignore
    case msg: ShardPutBulk => ???
    // case msg: ShardPutBulk =>
    //   ZIO.foreach_(msg.a)(v =>
    //     ZIO.effect(kvs.el.put(ElKey(v._1), v._2))
    //   ).ignore *> ZIO.accessM(_.replyToSender(()).ignore)
    case msg: ShardRemove => ZIO.effect(kvs.remove(msg.fid, msg.id)).ignore
  }

  // def extract[A: Codec](en: En): IO[KvsErr.Decode, A] = decode[A](en.data.unsafeArray).mapError(KvsErr.Decode)
  // def extractOpt[A: Codec](en: Option[En]): IO[KvsErr.Decode, Option[A]] = en match {
  //   case Some(b) => extract(b).asSome
  //   case None => ZIO.none
  // }
  // def bytes(a: String): Bytes = Bytes.unsafeWrap(a.getBytes("UTF-8"))
  // def bytes[A: Codec](a: A): Bytes = Bytes.unsafeWrap(encode(a))

  // val kvserr: zd.kvs.Err => KvsErr = {
  //   case zd.kvs.EntryExists(key) => KvsErr.EntryExists(key)
  //   case zd.kvs.FileNotExists(path) => KvsErr.FileNotExists(path)
  //   case zd.kvs.FileAlreadyExists(path) => KvsErr.FileAlreadyExists(path)
  //   case zd.kvs.Fail(r) => KvsErr.Fail(r)
  //   case zd.kvs.Throwed(e) => KvsErr.Throwed(e)
  //   case zd.kvs.AckQuorumFailed(why) => KvsErr.AckQuorumFailed(why)
  //   case zd.kvs.AckTimeoutFailed(op, k) => KvsErr.AckTimeoutFailed(op, k)
  // }
}
