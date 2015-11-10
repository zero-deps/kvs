package mws.kvs

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FunSuiteLike, Matchers}

class ActorTest(config: Config = ConfigFactory.parseString("")) extends TestKit(ActorSystem("TestSystem",config)) with FunSuiteLike
  with ImplicitSender
  with Matchers