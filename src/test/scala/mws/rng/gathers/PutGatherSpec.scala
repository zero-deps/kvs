package mws.rng.gathers

import akka.actor.{Address, ActorSystem}
import akka.testkit.{TestFSMRef, ImplicitSender, TestKit}
import akka.util.ByteString
import mws.rng._
import org.scalatest.{MustMatchers, BeforeAndAfterAll, WordSpecLike}


abstract class FSMSpec extends TestKit(ActorSystem()) with ImplicitSender
with WordSpecLike with MustMatchers with BeforeAndAfterAll {
  override def afterAll = TestKit.shutdownActorSystem(system)
}

object PutGatherSpec{

  
}

class PutGatherSpec extends FSMSpec{

  
}
