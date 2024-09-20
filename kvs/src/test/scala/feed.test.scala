package kvs
package feed

import zd.rng.*
import proto.*
import scala.concurrent.duration.*
import zio.*
import zio.test.*, Assertion.*

case class Entry(@N(1) i: Int)

given MessageCodec[Entry] = caseCodecAuto

object FeedSpec extends ZIOSpecDefault:
  val name = "test"
  val pekkoConf: ULayer[ActorSystem.Conf] =
    ActorSystem.staticConf(name, zd.rng.pekkoConf(name, "127.0.0.1", 4344) ++ "pekko.loglevel=off")
  val actorSystem: TaskLayer[ActorSystem] =
    pekkoConf >>> ActorSystem.layer
  val dbaConf: ULayer[Conf] =
    ZLayer.succeed(zd.rng.Conf(dir = s"target/data-${java.util.UUID.randomUUID}"))
  val dba: TaskLayer[Dba] =
    actorSystem ++ dbaConf >>> Rng.layer
  val feedLayer: TaskLayer[Feed] =
    actorSystem ++ dba >>> kvs.feed.layer
  given CanEqual[None.type, Option[Value]] = CanEqual.derived

  def spec = suite("FeedSpec")(
    test("FILO") {
      val fid = "feed1"
      for
        _ <- add(fid, Entry(1))
        _ <- add(fid, Entry(2))
        _ <- add(fid, Entry(3))
        a1 <- get(fid, 1).map(_.map(_.i))
        a2 <- get(fid, 2).map(_.map(_.i))
        a3 <- get(fid, 3).map(_.map(_.i))
        xs <- all(fid).map(_._2.i).runCollect
      yield
        assert(xs)(equalTo(Seq(3, 2, 1))) &&
        assert(a1)(equalTo(Some(1))) &&
        assert(a2)(equalTo(Some(2))) &&
        assert(a3)(equalTo(Some(3)))
    }
  , test("remove head") {
      val fid = "feed2"
      for
        _ <- add(fid, Entry(1))
        _ <- add(fid, Entry(2))
        _ <- add(fid, Entry(3))
        _ <- remove(fid, 3)
        xs <- all(fid).map(_._2.i).runCollect
        a <- get(fid, 3)
      yield assert(xs)(equalTo(Seq(2, 1))) && assert(a)(equalTo(None))
    }
  , test("remove last") {
      val fid = "feed3"
      for
        _ <- add(fid, Entry(1))
        _ <- add(fid, Entry(2))
        _ <- add(fid, Entry(3))
        _ <- remove(fid, 1)
        xs <- all(fid).map(_._2.i).runCollect
        a <- get(fid, 1)
      yield assert(xs)(equalTo(Seq(3, 2))) && assert(a)(equalTo(None))
    }
  , test("remove entry") {
      val fid = "feed4"
      for
        _ <- add(fid, Entry(1))
        _ <- add(fid, Entry(2))
        _ <- add(fid, Entry(3))
        _ <- remove(fid, 2)
        xs <- all(fid).map(_._2.i).runCollect
        a <- get(fid, 2)
      yield assert(xs)(equalTo(Seq(3, 1))) && assert(a)(equalTo(None))
    }
  , test("remove all entries") {
      val fid = "feed5"
      for
        _ <- add(fid, Entry(1))
        _ <- add(fid, Entry(2))
        _ <- add(fid, Entry(3))
        _ <- remove(fid, 2)
        _ <- remove(fid, 1)
        _ <- remove(fid, 3)
        xs <- all(fid).runCollect
      yield assert(xs)(equalTo(Nil))
    }
  ).provideLayerShared(feedLayer)
end FeedSpec
