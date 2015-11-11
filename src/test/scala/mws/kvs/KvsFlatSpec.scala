package mws.kvs

import org.scalatest._
import org.sclatest.matchers._

import akka.actor.{ActorSystem}
import akka.testkit._
import akka.testkit.TestKit
import akka.testkit.TestEvent._
import com.typesafe.config.ConfigFactory

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
}
