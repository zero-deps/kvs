package zd.kvs

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import zd.kvs.Kvs
import zd.proto._, api._, macrosapi._
import zero.ext._, traverse._

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

  // codecs
  implicit val usersc = caseCodecAuto[Users]
  implicit val userc = caseCodecAuto[User]

  // Run kvs
  val kvs = Kvs(system)
  Try(Await.result(kvs.onReady, Duration.Inf))

  // Add users to feed
  val users = FdKey(encodeToBytes(Users(System.currentTimeMillis)))
  kvs.add(users, encodeToBytes(User(name="John Doe")))
  kvs.add(users, encodeToBytes(User(name="Jane Doe")))

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

sealed trait Feed
@N(1) final case class Users(@N(1) time: Long) extends Feed