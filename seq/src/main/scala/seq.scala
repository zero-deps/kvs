package kvs

import com.typesafe.config.Config
import zio._, stream._
import zio.akka.cluster.sharding.{Sharding, Entity}
import zd.proto.Bytes
import zero.ext._, option._

import store._

sealed trait ShardMsg
final case class ShardAdd1   (fid: FdKey,            data: Bytes) extends ShardMsg
final case class ShardAdd    (fid: FdKey, id: ElKey, data: Bytes) extends ShardMsg
final case class ShardPut    (fid: FdKey, id: ElKey, data: Bytes) extends ShardMsg
final case class ShardRemove (fid: FdKey, id: ElKey)              extends ShardMsg
final case class ShardCleanup(fid: FdKey)                         extends ShardMsg
final case class ShardFix    (fid: FdKey)                         extends ShardMsg
// final case class ShardPutBulk(fid: FdKey, a: Vector[(ElKey, Bytes)]) extends ShardMsg

trait DataCodec[A] {
  def extract(xs: Bytes): A
  def insert(x: A): Bytes
}

package object seq {
  type ActorSystem = Has[_root_.akka.actor.ActorSystem]
  type Kvs = Has[Kvs.Service]
  type KIO[A] = IO[Err, A]
  type KZIO[A] = ZIO[Kvs, Err, A]
  type KURIO[A] = URIO[Kvs, A]
  type KStream[A] = Stream[Err, A]

  def actorSystem(name: String): TaskLayer[ActorSystem] = ZLayer.fromManaged{
    ZIO.effect(_root_.akka.actor.ActorSystem(name)).toManaged(as => Task.fromFuture(_ => as.terminate()).either)
  }
  def actorSystem(name: String, config: Config): TaskLayer[ActorSystem] = ZLayer.fromManaged{
    ZIO.effect(_root_.akka.actor.ActorSystem(name, config)).toManaged(as => Task.fromFuture(_ => as.terminate()).either)
  }

  object Kvs {
    trait Service {
      def all    [A: DataCodec](fid: FdKey, after: Option[ElKey]): KStream[(ElKey, A)]
      def apply  [A: DataCodec](fid: FdKey, id: ElKey      ): KIO[A]
      def get    [A: DataCodec](fid: FdKey, id: ElKey      ): KIO[Option[A]]
      def head   [A: DataCodec](fid: FdKey                 ): KIO[Option[(ElKey, A)]]
      def prepend[A: DataCodec](fid: FdKey,            a: A): KIO[ElKey]
      def prepend[A: DataCodec](fid: FdKey, id: ElKey, a: A): KIO[Unit]
      def put    [A: DataCodec](fid: FdKey, id: ElKey, a: A): KIO[Unit]
      def remove [A: DataCodec](fid: FdKey, id: ElKey      ): KIO[Boolean]
      def cleanup              (fid: FdKey                 ): KIO[Unit]
      def fix                  (fid: FdKey                 ): KIO[Unit]
      val array: ArrayApi
      // def putBulk[A: DataCodec](fid: FdKey, a: Vector[(ElKey, A)]): ZIO[Blocking with Clock, Err, Unit]
    }
    trait ArrayApi {
      def all[A: DataCodec](fid: FdKey, size: Long): KStream[A]
      def put[A: DataCodec](fid: FdKey, size: Long, data: A): KIO[Unit]
    }
    trait ArrayAccess {
      def all[A: DataCodec](fid: FdKey, size: Long): KURIO[KStream[A]]
      def put[A: DataCodec](fid: FdKey, size: Long, data: A): KZIO[Unit]
    }

    def all    [A: DataCodec](fid: FdKey, after: Option[ElKey]=none): KURIO[KStream[(ElKey, A)]] = ZIO.access(_.get.all[A](fid, after))
    def apply  [A: DataCodec](fid: FdKey, id: ElKey      ): KZIO[A]                  = ZIO.accessM(_.get.apply[A] (fid, id))
    def get    [A: DataCodec](fid: FdKey, id: ElKey      ): KZIO[Option[A]]          = ZIO.accessM(_.get.get[A]   (fid, id))
    def head   [A: DataCodec](fid: FdKey                 ): KZIO[Option[(ElKey, A)]] = ZIO.accessM(_.get.head[A]  (fid))
    def prepend[A: DataCodec](fid: FdKey,            a: A): KZIO[ElKey]              = ZIO.accessM(_.get.prepend  (fid, a))
    def prepend[A: DataCodec](fid: FdKey, id: ElKey, a: A): KZIO[Unit]               = ZIO.accessM(_.get.prepend  (fid, id, a))
    def put    [A: DataCodec](fid: FdKey, id: ElKey, a: A): KZIO[Unit]               = ZIO.accessM(_.get.put      (fid, id, a))
    def remove [A: DataCodec](fid: FdKey, id: ElKey      ): KZIO[Boolean]            = ZIO.accessM(_.get.remove[A](fid, id))
    def cleanup              (fid: FdKey                 ): KZIO[Unit]               = ZIO.accessM(_.get.cleanup  (fid))
    def fix                  (fid: FdKey                 ): KZIO[Unit]               = ZIO.accessM(_.get.fix      (fid))
    val array = new ArrayAccess {
      def all[A: DataCodec](fid: FdKey, size: Long): KURIO[KStream[A]] = ZIO.access(_.get.array.all[A](fid, size))
      def put[A: DataCodec](fid: FdKey, size: Long, data: A): KZIO[Unit] = ZIO.accessM(_.get.array.put[A](fid, size, data))
    }
    // def putBulk[A: DataCodec](fid: FdKey, a: Vector[(ElKey, A)]): ZIO[Kvs with Blocking with Clock, Err, Unit] = ZIO.accessM(_.get.putBulk(fid, a))

    def live(conf: store.DbaConf): ZLayer[ActorSystem, Err, Kvs] = {
      def onMessage(implicit dba: Dba): ShardMsg => ZIO[Entity[Unit], Nothing, Unit] = {
        case msg: ShardAdd1    => ZIO.accessM[Entity[Unit]](_.get.replyToSender(feed.prepend(      msg.fid,          msg.data)).orDie)
        case msg: ShardAdd     => ZIO.accessM[Entity[Unit]](_.get.replyToSender(feed.prepend(EnKey(msg.fid, msg.id), msg.data)).orDie)
        case msg: ShardPut     => ZIO.accessM[Entity[Unit]](_.get.replyToSender(feed.put    (EnKey(msg.fid, msg.id), msg.data)).orDie)
        case msg: ShardRemove  => ZIO.accessM[Entity[Unit]](_.get.replyToSender(feed.remove (EnKey(msg.fid, msg.id)          )).orDie)
        case msg: ShardCleanup => ZIO.accessM[Entity[Unit]](_.get.replyToSender(feed.cleanup(      msg.fid                   )).orDie)
        case msg: ShardFix     => ZIO.accessM[Entity[Unit]](_.get.replyToSender(feed.fix    (      msg.fid                   )).orDie)
        // case msg: ShardPutBulk =>
        //   ZIO.foreach_(msg.a)(v =>
        //     ZIO.effect(kvs.el.put(v._1, v._2))
        //   ).ignore *> ZIO.accessM(_.get.replyToSender(()).ignore)
      }
      ZLayer.fromEffect{
        for {
          as   <- ZIO.access[ActorSystem](_.get)
          dba  <- conf match {
                    case RngConf(conf) => ZIO.effect(Rng(as, conf))
                    case MemConf       => ZIO.effect(Mem())
                    case _             => ZIO.fail(new Exception("ni"))
                  }
          sh   <- Sharding.start("write_shard", onMessage(dba)).provideLayer(ZLayer.succeed(as))
          res  <- ZIO.succeed(new Service {
                    private implicit val dba1 = dba
                    /* readonly api */
                    def all[A: DataCodec](fid: FdKey, after: Option[ElKey]): KStream[(ElKey, A)] = {
                      Stream
                        .fromIterableM(IO.fromEither(feed.all(fid, after)))
                        .mapM(IO.fromEither(_))
                        .mapM(x => IO.effect(implicitly[DataCodec[A]].extract(x._2)).orDie.map(x._1 -> _))
                    }
                    def apply[A: DataCodec](fid: FdKey, id: ElKey): KIO[A] = {
                      for {
                        res  <- IO.fromEither(feed.apply(EnKey(fid, id)))
                        a    <- IO.effect(implicitly[DataCodec[A]].extract(res)).orDie
                      } yield a
                    }
                    def get[A: DataCodec](fid: FdKey, id: ElKey): KIO[Option[A]] = {
                      for {
                        res  <- IO.fromEither(feed.get(EnKey(fid, id)))
                        a    <- res.cata(res => IO.effect(implicitly[DataCodec[A]].extract(res)).map(_.some).orDie, IO.none)
                      } yield a
                    }
                    def head[A: DataCodec](fid: FdKey): KIO[Option[(ElKey, A)]] = {
                      for {
                        res  <- IO.fromEither(feed.head(fid))
                        a    <- res.cata(res => IO.effect(implicitly[DataCodec[A]].extract(res._2)).map(res._1 -> _).map(_.some).orDie, IO.none)
                      } yield a
                    }
                    /* writable api */
                    def prepend[A: DataCodec](fid: FdKey, a: A): KIO[ElKey] = {
                      val encoded = implicitly[DataCodec[A]].insert(a)
                      sh.ask[Res[ElKey]](hex(fid), ShardAdd1(fid, encoded)).mapError(Throwed).flatMap(IO.fromEither(_))
                    }
                    def prepend[A: DataCodec](fid: FdKey, id: ElKey, a: A): KIO[Unit] = {
                      val encoded = implicitly[DataCodec[A]].insert(a)
                      sh.ask[Res[Unit]](hex(fid), ShardAdd(fid, id, encoded)).mapError(Throwed).flatMap(IO.fromEither(_))
                    }
                    def put[A: DataCodec](fid: FdKey, id: ElKey, a: A): KIO[Unit] = {
                      val encoded = implicitly[DataCodec[A]].insert(a)
                      sh.ask[Res[Unit]](hex(fid), ShardPut(fid, id, encoded)).mapError(Throwed).flatMap(IO.fromEither(_))
                    }
                    def remove[A: DataCodec](fid: FdKey, id: ElKey): KIO[Boolean] = {
                      sh.ask[Res[Boolean]](hex(fid), ShardRemove(fid, id)).mapError(Throwed).flatMap(IO.fromEither(_))
                    }
                    def cleanup(fid: FdKey): KIO[Unit] = {
                      sh.ask[Res[Unit]](hex(fid), ShardCleanup(fid)).mapError(Throwed).flatMap(IO.fromEither(_))
                    }
                    def fix(fid: FdKey): KIO[Unit] = {
                      sh.ask[Res[Unit]](hex(fid), ShardCleanup(fid)).mapError(Throwed).flatMap(IO.fromEither(_))
                    }
                    /* array */
                    val array = new ArrayApi {
                      def all[A: DataCodec](fid: FdKey, size: Long): KStream[A] = {
                        Stream
                          .fromIterableM(IO.fromEither(kvs.array.all(fid, size)))
                          .mapM(IO.fromEither(_))
                          .mapM(x => IO.effect(implicitly[DataCodec[A]].extract(x)).orDie)
                      }
                      def put[A: DataCodec](fid: FdKey, size: Long, data: A): KIO[Unit] = ???
                    }
                    // def putBulk[A: DataCodec](fid: FdKey, a: Vector[(ElKey, A)]): ZIO[Blocking with Clock, Err, Unit] = {
                    //   import blocking.{blocking => succeedb, _}
                    //   val c = implicitly[DataCodec[A]]
                    //   for {
                    //     b  <- succeedb(UIO.succeed(a.map(kv => (kv._1, c.insert(kv._2))))).timed
                    //     _  <- UIO.succeed(println(s"enc=${b._1.toMillis}"))
                    //     r  <- sh.ask[Unit](hex(fid), ShardPutBulk(fid, b._2)).mapError(Throwed).timed
                    //     _  <- UIO.succeed(println(s"put=${r._1.toMillis}"))
                    //   } yield ()
                    // }
                  })
        } yield res
      }.mapError(Throwed)
    }
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
