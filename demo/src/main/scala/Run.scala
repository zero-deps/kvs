package zd.kvs

import zd.gs.z._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import zd.kvs.Kvs
import scala.util.Try
import scala.concurrent.Await
import scala.concurrent.duration._
import zd.proto.Bytes
import zd.proto.api.{N, MessageCodec, encode, decode}
import zd.proto.macrosapi.caseCodecAuto
import zd.gs.z._

object Run extends App {

  // Create actor system for kvs
  val asname = "sys"
  val nodeipaddr = "127.0.0.1"
  val nodeport = "4343"
  val cfg = s"""
    |akka.remote.netty.tcp.hostname = $nodeipaddr
    |akka.remote.netty.tcp.port = $nodeport
    |akka.cluster.seed-nodes = [ "akka.tcp://$asname@$nodeipaddr:$nodeport" ]
    |ring.leveldb.dir = rng_data
    |""".stripMargin
  val system = ActorSystem(asname, ConfigFactory.parseString(cfg))

  // Run kvs
  val kvs = Kvs(system)
  Try(Await.result(kvs.onReady, Duration.Inf))

  // Add users to feed
  val users = Bytes.unsafeWrap(s"users${System.currentTimeMillis}".getBytes)
  implicit val UserC: MessageCodec[User] = caseCodecAuto[User]
  kvs.add(users, Bytes.unsafeWrap(encode[User](User(name="John Doe"))))
  kvs.add(users, Bytes.unsafeWrap(encode[User](User(name="Jane Doe"))))

  // Get all users
  kvs.all(users).flatMap(_.sequence).fold(
    err => system.log.error(err.toString)
  , xs => xs.foreach(x => system.log.info(decode[User](x.en.data.unsafeArray).name))
  )

  // Stop kvs
  system.terminate()
  Try(Await.result(system.whenTerminated, Duration.Inf))

}

final case class User(@N(1) name: String)