package zd.kvs

import akka.actor.ActorSystem
import akka.cluster.{Cluster, MemberStatus}
import akka.serialization.SerializationExtension
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import proto.*
import scala.concurrent.duration.*
import zd.kvs.idx.IdxHandler
import zd.kvs.idx.IdxHandler.given
import zio.*

final case class FeedAdd(@N(1) feed: String, @N(2) entryId: String)
given MessageCodec[FeedAdd] = caseCodecAuto

class SeqConsistencyTest
    extends TestKit(ActorSystem(
      "seq-consistency-test"
    , ConfigFactory.parseString("""
        akka.actor.provider = cluster
        akka.remote.artery.canonical.hostname = "127.0.0.1"
        akka.remote.artery.canonical.port = 0
        akka.actor.allow-java-serialization = off
        akka.loglevel = off
      """).withFallback(ConfigFactory.load())
    ))
    with AnyFreeSpecLike
    with Matchers
    with BeforeAndAfterAll:

  private val kvs = Kvs.mem()

  private def unsafeRun[A](effect: ZIO[Any, Any, A]): A =
    Unsafe.unsafely(Runtime.default.unsafe.run(effect).getOrThrowFiberFailure())

  Cluster(system).join(Cluster(system).selfAddress)
  awaitAssert(Cluster(system).selfMember.status shouldBe MemberStatus.Up, 10.seconds, 100.millis)

  private val config = SeqConsistency.Config[FeedAdd, IdxHandler.Idx](
    name = "test-index-feeds"
  , handler = add => ZIO.fromEither(kvs.index.add(IdxHandler.Idx(IdxHandler.Fid(add.feed), add.entryId)))
  , entityId = _.feed
  )

  private val consistency = unsafeRun:
    ZIO.service[SeqConsistency].provide(
      SeqConsistency.layer
    , ClusterSharding.layer
    , ZLayer.succeed(config)
    , ZLayer.succeed(system)
    )

  private val sharding = unsafeRun:
    ZIO.service[ClusterSharding].provide(
      ClusterSharding.layer
    , ZLayer.succeed(system)
    )

  "SeqConsistency" - {
    "uses the protobuf serializer for every wire envelope" in {
      val serialization = SerializationExtension(system)
      val messages: List[ShardingMessage] = List(
        ShardRequest("feed", Array[Byte](1, 2, 3))
      , ShardSuccess(Array[Byte](4, 5, 6))
      , ShardFailure(WireError("EntryExists", "feed.1" :: Nil))
      , ShardDefect("defect")
      )

      messages.foreach: message =>
        val serializer = serialization.findSerializerFor(message)
        serializer shouldBe a[ShardingSerializer]
        val bytes = serialization.serialize(message).get
        val decoded = serialization.deserialize[ShardingMessage](bytes, serializer.identifier, None).get
        (message, decoded) match
          case (ShardRequest(id, payload), ShardRequest(decodedId, decodedPayload)) =>
            decodedId shouldBe id
            decodedPayload.toSeq shouldBe payload.toSeq
          case (ShardSuccess(payload), ShardSuccess(decodedPayload)) =>
            decodedPayload.toSeq shouldBe payload.toSeq
          case (expected, actual) => actual shouldBe expected
    }

    "serializes concurrent updates for each feed" in {
      val feeds = "alpha" :: "beta" :: Nil
      unsafeRun:
        ZIO.foreachParDiscard(feeds): feed =>
          ZIO.foreachParDiscard(1 to 50): n =>
            consistency.send[IdxHandler.Idx](FeedAdd(feed, n.toString))

      feeds.foreach: feed =>
        val entries = kvs.index.all(IdxHandler.Fid(feed)).toOption.toList.flatMap(_.flatMap(_.toOption))
        entries.map(_.id).toSet shouldBe (1 to 50).map(_.toString).toSet
    }

    "propagates feed errors" in {
      val result = unsafeRun(consistency.send[IdxHandler.Idx](FeedAdd("alpha", "1")).either)
      result shouldBe Left(EntryExists("alpha.1"))
    }

    "fails instead of hanging on an unexpected reply" in {
      val exit = unsafeRun:
        for
          region <- sharding.start(
            "unexpected-reply"
          , akka.actor.Props(new UnexpectedReply)
          )
          exit <- sharding
            .send(region, ShardRequest("request", Array.emptyByteArray))
            .exit
            .timeoutFail(RuntimeException("timed out"))(zio.Duration.fromSeconds(3))
        yield exit
      exit.isFailure shouldBe true
    }

    "stops the temporary receiver when the request is interrupted" in {
      val probe = TestProbe()
      val region = unsafeRun(sharding.start("never-reply", akka.actor.Props(new NeverReply(probe.ref))))
      val fiber = unsafeRun(sharding.send(region, ShardRequest("request", Array.emptyByteArray)).forkDaemon)
      val receiver = probe.expectMsgType[akka.actor.ActorRef](scala.concurrent.duration.Duration(3, "seconds"))
      watch(receiver)
      unsafeRun(fiber.interrupt)
      expectTerminated(receiver, scala.concurrent.duration.Duration(3, "seconds"))
    }
  }

  override def afterAll(): Unit =
    kvs.close()
    TestKit.shutdownActorSystem(system)
end SeqConsistencyTest

private final class UnexpectedReply extends akka.actor.Actor:
  def receive: Receive =
    case _ => sender() ! "unexpected"
end UnexpectedReply

private final class NeverReply(probe: akka.actor.ActorRef) extends akka.actor.Actor:
  def receive: Receive =
    case _: ShardRequest => probe ! sender()
end NeverReply
