package mws.kvs

import org.scalatest._
import org.scalatest.matchers._
import org.scalatest.concurrent._
import org.scalatest.concurrent.ScalaFutures._

import org.scalactic._

import akka.actor.ActorSystem
import akka.testkit._
import akka.testkit.TestKit
import akka.testkit.TestEvent._

import com.typesafe.config._

import store._
import handle._

class HandleSpec(_system:ActorSystem) extends TestKit(_system)
  with DefaultTimeout
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("handle", ConfigFactory.load))

  implicit val kvs:Kvs = Kvs(system)

  override def afterAll = {kvs.close;TestKit.shutdownActorSystem(system)}

  import SessionHandler._

  "kvs" should "have the configured and ready" in {
    whenReady(kvs.isReady){r => r should be (true)}
  }
}
