package zd.kvs

object Run extends App {

  import zd.gs.z._
  import akka.actor.ActorSystem
  val cfg = """akka.remote.netty.tcp.hostname = 127.0.0.1
  akka.remote.netty.tcp.port = 4343
  akka.cluster.seed-nodes = [ "akka.tcp://sys@127.0.0.1:4343" ]
  ring.leveldb.dir = rng_data_127.0.0.1_4343"""
  import com.typesafe.config.ConfigFactory
  val system = ActorSystem("sys", ConfigFactory.parseString(cfg))
  import zd.kvs.Kvs
  val kvs = Kvs(system)
  import scala.util.Try
  import scala.concurrent.Await
  import scala.concurrent.duration._
  Try(Await.result(kvs.onReady, Duration.Inf))
  import zd.proto.Bytes
  val users = Bytes.unsafeWrap(s"users${System.currentTimeMillis}".getBytes)
  import zd.proto.api.{N, MessageCodec, encode, decode}
  final case class User(@N(1) name: String)
  import zd.proto.macrosapi.caseCodecAuto
  implicit val UserC: MessageCodec[User] = caseCodecAuto[User]
  kvs.add(users, Bytes.unsafeWrap(encode[User](User(name="John Doe"))))
  kvs.add(users, Bytes.unsafeWrap(encode[User](User(name="Jane Doe"))))
  import zd.gs.z._
  kvs.all(users).flatMap(_.sequence).fold(
      err => system.log.error(err.toString)
    , xs => xs.foreach(x => system.log.info(decode[User](x.en.data.unsafeArray).name))
    )
  system.terminate()
  Try(Await.result(system.whenTerminated, Duration.Inf))

}
