package kvs.feed
package typed

import zio.*, clock.*
import proto.*
import zio.test.*, Assertion.*
import kvs.rng.{ActorSystem, Dba}
import scala.language.postfixOps
import scala.concurrent.duration.*

case class Entry(@N(1) i: Int)

given MessageCodec[Entry] = caseCodecAuto

object FeedSpec extends DefaultRunnableSpec:
  val name = "test"
  val akkaConf: ULayer[Has[ActorSystem.Conf]] =
    ActorSystem.staticConf(name, kvs.rng.akkaConf(name, "127.0.0.1", 4344) ++ "akka.loglevel=off")
  val actorSystem: TaskLayer[ActorSystem] =
    akkaConf >>> ActorSystem.live
  val dbaConf: ULayer[Has[kvs.rng.Conf]] =
    ZLayer.fromEffect(ZIO.succeed(kvs.rng.Conf(dir = s"target/data-${java.util.UUID.randomUUID}")))
  val dba: TaskLayer[Dba] =
    actorSystem ++ dbaConf ++ Clock.live >>> Dba.live
  val feedLayer: TaskLayer[Feed] =
    actorSystem ++ dba >>> Feed.live

  def spec = suite("FeedSpec")(
    testM("FILO") {
      val fid = "feed"
      (for
        _ <- add(fid, Entry(1))
        _ <- add(fid, Entry(2))
        xs <- all(fid).map(_._2.i).runCollect
      yield assert(xs)(equalTo(Seq(2, 1)))).provideLayer(Clock.live ++ feedLayer)
    }
  )