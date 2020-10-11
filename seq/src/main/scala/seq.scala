package zero.kvs

import com.typesafe.config.Config
import zio._, stream._, blocking.{blocking => succeedb, _}, clock._
import zio.akka.cluster.sharding.{Sharding, Entity}
import zd.proto._
import zd.kvs._

sealed trait ShardMsg
final case class ShardAdd1(fid: FdKey, data: Bytes) extends ShardMsg
final case class ShardAdd(fid: FdKey, id: ElKey, data: Bytes) extends ShardMsg
final case class ShardPut(fid: FdKey, id: ElKey, data: Bytes) extends ShardMsg
final case class ShardPutBulk(fid: FdKey, a: Vector[(ElKey, Bytes)]) extends ShardMsg
final case class ShardRemove(fid: FdKey, id: ElKey) extends ShardMsg

package object seq {
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

  implicit object BytesDataCodec extends DataCodec[Bytes] {
    def extract(x: Bytes): Bytes = x
    def insert(x: Bytes): Bytes = x
  }

  object Kvs {
    trait Service {
      def apply[A: DataCodec](fid: FdKey, id: ElKey): KIO[A]
      def get[A: DataCodec](fid: FdKey, id: ElKey): KIO[Option[A]]
      def add[A: DataCodec](fid: FdKey, a: A): KIO[A]
      def add[A: DataCodec](fid: FdKey, id: ElKey, a: A): KIO[A]
      def put[A: DataCodec](fid: FdKey, id: ElKey, a: A): KIO[A]
      def putBulk[A: DataCodec](fid: FdKey, a: Vector[(ElKey, A)]): ZIO[Blocking with Clock, Err, Unit]
      def remove[A: DataCodec](fid: FdKey, id: ElKey): KIO[Unit]
      def stream[A: DataCodec](fid: FdKey, after: Option[ElKey]): Stream[Err, (ElKey, A)]
      def cleanup(fid: FdKey): KIO[Unit]
    }

    def apply[A: DataCodec](fid: FdKey, id: ElKey): ZIO[Kvs, Err, A] = ZIO.accessM(_.get.apply[A](fid, id))
    def get[A: DataCodec](fid: FdKey, id: ElKey): ZIO[Kvs, Err, Option[A]] = ZIO.accessM(_.get.get[A](fid, id))
    def add[A: DataCodec](fid: FdKey, a: A): KZIO[Kvs, A] = ZIO.accessM(_.get.add(fid, a))
    def add[A: DataCodec](fid: FdKey, id: ElKey, a: A): KZIO[Kvs, A] = ZIO.accessM(_.get.add(fid, id, a))
    def put[A: DataCodec](fid: FdKey, id: ElKey, a: A): ZIO[Kvs, Err, A] = ZIO.accessM(_.get.put(fid, id, a))
    def putBulk[A: DataCodec](fid: FdKey, a: Vector[(ElKey, A)]): ZIO[Kvs with Blocking with Clock, Err, Unit] = ZIO.accessM(_.get.putBulk(fid, a))
    def remove[A: DataCodec](fid: FdKey, id: ElKey): ZIO[Kvs, Err, Unit] = ZIO.accessM(_.get.remove[A](fid, id))
    def stream[A: DataCodec](fid: FdKey, after: Option[ElKey] = None): ZIO[Kvs, Nothing, Stream[Err, (ElKey, A)]] = ZIO.access(_.get.stream[A](fid, after))
    def cleanup(fid: FdKey): ZIO[Kvs, Nothing, Unit] = ZIO.access(x => { x.get.cleanup(fid); () })

    val live: ZLayer[ActorSystem, Err, Kvs] = ZLayer.fromEffect{
      for {
        as   <- ZIO.access[ActorSystem](_.get)
        kvs  <- ZIO.effect(zd.kvs.Kvs(as))
        sh   <- Sharding.start("write_shard", onMessage=writeShard(kvs)).provideLayer(ZLayer.succeed(as))
        res  <- ZIO.succeed(new Service {
                  def apply[A: DataCodec](fid: FdKey, id: ElKey): KIO[A] = for {
                    res  <- IO.fromEither(kvs.apply[A](fid, id))
                  } yield res

                  def get[A: DataCodec](fid: FdKey, id: ElKey): KIO[Option[A]] = for {
                    res  <- IO.fromEither(kvs.get[A](fid, id))
                  } yield res

                  def add[A: DataCodec](fid: FdKey, a: A): KIO[A] = {
                    val encoded = implicitly[DataCodec[A]].insert(a)
                    sh.send(hex(fid), ShardAdd1(fid, encoded)).as(a).mapError(Throwed)
                  }

                  def add[A: DataCodec](fid: FdKey, id: ElKey, a: A): KIO[A] = {
                    val encoded = implicitly[DataCodec[A]].insert(a)
                    sh.send(hex(fid), ShardAdd(fid, id, encoded)).as(a).mapError(Throwed)
                  }
                  
                  def put[A: DataCodec](fid: FdKey, id: ElKey, a: A): KIO[A] = {
                    val encoded = implicitly[DataCodec[A]].insert(a)
                    sh.send(hex(fid), ShardPut(fid, id, encoded)).as(a).mapError(Throwed)
                  }

                  def putBulk[A: DataCodec](fid: FdKey, a: Vector[(ElKey, A)]): ZIO[Blocking with Clock, Err, Unit] = {
                    val c = implicitly[DataCodec[A]]
                    for {
                      b  <- succeedb(UIO.succeed(a.map(kv => (kv._1, c.insert(kv._2))))).timed
                      _  <- UIO.succeed(println(s"enc=${b._1.toMillis}"))
                      r  <- sh.ask[Unit](hex(fid), ShardPutBulk(fid, b._2)).mapError(Throwed).timed
                      _  <- UIO.succeed(println(s"put=${r._1.toMillis}"))
                    } yield ()
                  }

                  def remove[A: DataCodec](fid: FdKey, id: ElKey): KIO[Unit] =
                    sh.send(hex(fid), ShardRemove(fid, id)).mapError(Throwed)

                  def stream[A: DataCodec](fid: FdKey, after: Option[ElKey]): Stream[Err, (ElKey, A)] = 
                    Stream
                      .fromIterableM(IO.fromEither(kvs.all[A](fid, after)))
                      .mapM(IO.fromEither(_))
                    
                  def cleanup(fid: FdKey): KIO[Unit] = IO.fromEither(kvs.cleanup(fid))
                })
      } yield res
    }.mapError(Throwed(_))
  }

  def writeShard(kvs: zd.kvs.Kvs): ShardMsg => ZIO[Entity[Unit], Nothing, Unit] = {
    case msg: ShardAdd1 => ZIO.effect(kvs.add(msg.fid, msg.data)).ignore
    case msg: ShardAdd => ZIO.effect(kvs.add(msg.fid, msg.id, msg.data)).ignore
    case msg: ShardPut => ZIO.effect(kvs.put(msg.fid, msg.id, msg.data)).ignore
    case msg: ShardPutBulk =>
      ZIO.foreach_(msg.a)(v =>
        ZIO.effect(kvs.el.put(v._1, v._2))
      ).ignore *> ZIO.accessM(_.get.replyToSender(()).ignore)
    case msg: ShardRemove => ZIO.effect(kvs.remove(msg.fid, msg.id)).ignore
  }

  private val hexs = "0123456789abcdef".getBytes("ascii")
  private def hex(fid: FdKey): String = {
    val bytes = fid.bytes
    val hexChars = new Array[Byte](bytes.length * 2)
    var i = 0
    while (i < bytes.length) {
        val v = bytes.unsafeArray(i) & 0xff
        hexChars(i * 2) = hexs(v >>> 4)
        hexChars(i * 2 + 1) = hexs(v & 0x0f)
        i = i + 1
    }
    new String(hexChars, "utf8")
  }
}
