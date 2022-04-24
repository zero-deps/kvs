package kvs.feed

import kvs.rng.{ActorSystem, Dba}
import proto.*
import scala.concurrent.duration.*
import scala.language.postfixOps
import zio.*
import zio.test.*, Assertion.*
import zio.test.ZIOSpecDefault

case class Entry(@N(1) i: Int)

given MessageCodec[Entry] = caseCodecAuto

object FeedSpec extends ZIOSpecDefault:
  val name = "test"
  val akkaConf: ULayer[ActorSystem.Conf] =
    ActorSystem.staticConf(name, kvs.rng.akkaConf(name, "127.0.0.1", 4344) ++ "akka.loglevel=off")
  val actorSystem: TaskLayer[ActorSystem] =
    akkaConf >>> ActorSystem.live
  val dbaConf: ULayer[kvs.rng.Conf] =
    ZLayer.succeed(kvs.rng.Conf(dir = s"target/data-${java.util.UUID.randomUUID}"))
  val dba: TaskLayer[Dba] =
    actorSystem ++ dbaConf ++ Clock.live >>> Dba.live
  val feedLayer: TaskLayer[Feed] =
    actorSystem ++ dba >>> kvs.feed.live

  def spec = suite("FeedSpec")(
    test("FILO") {
      val fid = "feed"
      (for
        _ <- add(fid, Entry(1))
        _ <- add(fid, Entry(2))
        xs <- all(fid).map(_._2.i).runCollect
      yield assert(xs)(equalTo(Seq(2, 1)))).provideLayer(Clock.live ++ feedLayer)
    }
  )