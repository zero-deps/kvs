package zd.kvs

import akka.actor.ActorSystem
import akka.cluster.{Cluster, MemberStatus}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration.*
import zd.kvs.idx.IdxHandler
import zio.*

final case class FeedAdd(feed: String, entryId: String)

class SeqConsistencyTest
    extends TestKit(ActorSystem(
      "seq-consistency-test"
    , ConfigFactory.parseString("""
        akka.actor.provider = cluster
        akka.remote.artery.canonical.hostname = "127.0.0.1"
        akka.remote.artery.canonical.port = 0
        akka.actor.allow-java-serialization = on
        akka.loglevel = off
      """)
    ))
    with AnyFreeSpecLike
    with Matchers
    with BeforeAndAfterAll:

  private val kvs = Kvs.mem()

  private def unsafeRun[A](effect: ZIO[Any, Any, A]): A =
    Unsafe.unsafely(Runtime.default.unsafe.run(effect).getOrThrowFiberFailure())

  Cluster(system).join(Cluster(system).selfAddress)
  awaitAssert(Cluster(system).selfMember.status shouldBe MemberStatus.Up, 10.seconds, 100.millis)

  private val config = SeqConsistency.Config(
    name = "test-index-feeds"
  , handler = {
      case FeedAdd(feed, entryId) =>
        ZIO
          .fromEither(kvs.index.add(IdxHandler.Idx(IdxHandler.Fid(feed), entryId)))
          .map(identity[Any])
      case msg => ZIO.fail(InvalidArgument(s"Unsupported test operation: $msg"))
    }
  , entityId = {
      case FeedAdd(feed, _) => feed
      case msg => throw IllegalArgumentException(s"Unsupported test operation: $msg")
    }
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
          , _.toString
          )
          exit <- sharding
            .send[String, Nothing](region, "request")
            .exit
            .timeoutFail(RuntimeException("timed out"))(zio.Duration.fromSeconds(3))
        yield exit
      exit.isFailure shouldBe true
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
