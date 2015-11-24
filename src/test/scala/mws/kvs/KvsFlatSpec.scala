package mws.kvs

import org.scalatest._
import org.scalatest.matchers._

import akka.actor.{ActorSystem}
import akka.pattern.ask
import akka.testkit._
import akka.testkit.TestKit
import akka.testkit.TestEvent._
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._

class KvsFlatSpec(_system:ActorSystem) extends TestKit(_system)
  with DefaultTimeout
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with GivenWhenThen
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("st",ConfigFactory.parseString("""
    kvs {
      store=leveldb
    }
  """)))
  override def afterAll = TestKit.shutdownActorSystem(system)

  "kvs" should "have the configured backend" in {
    val c = system.settings.config
    val kvsc = c.getConfig("kvs")
    println(s"kvsc")
  }

  "a" should "a" in {
    import favorite._

    val at:Map[String,String] = Map.empty[String,String]
    val pr:Int = 1
    implicit val g = Game(at,pr)

    info(s"Empty Game:${g.toString}")

    g match {
      case Game(a,p) => info(s"Game unnaplied successfully $a, $p")
      case _ => error("game is failed to match")
    }

    info(s"${g._1}  ${g._2} ${Game.id(g)}")
    info(s"""${Game.withId("f1")}""")

    val g1 = Game.withId("f1")
    info(s"${Game.withPriority(2)(g1)}")

//    info(s"object type: ${Game.type}")
  }

  "FavHandler" should "operate with favs" in {
    import favorite._
    val fh = TestActorRef(new FavHandler)
    val ffh = fh ? "fuck"
    val m = Await.result(ffh, 500 milliseconds)
    m should be("ok")

    val ech = system.actorOf(FavHandler.props)
    ech ! "fuck"
    expectMsg("ok")
  }
}



















