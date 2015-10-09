package mws.rng

import akka.actor.{ActorSystem, ActorSelection, ActorRef}
import akka.testkit.{TestProbe, TestFSMRef}
import org.scalatest.{Matchers, WordSpecLike}


class GatherGetFsmTest extends WordSpecLike with Matchers {
  import  GatherGetFsmTest._

  val fsm = TestFSMRef(new GatherGetFsm(client.ref, 3,2,3, refMem))
  
  fsm.tell(GetResp(None), store1.ref)
  assertResult(Collecting)(fsm.stateName)
}

object GatherGetFsmTest{
  implicit val system = ActorSystem()

  val client = TestProbe()
  val store1 = TestProbe()

  val store2 = TestProbe()
  val store3 = TestProbe()
  
  val n1 = TestProbe()
  val n2 = TestProbe()
  val n3 = TestProbe()
  
  var refMem: ActorRefStorage = new Mem
}

class Mem extends ActorRefStorage {
  import  GatherGetFsmTest._
  val stores = Map(
    n1.ref.path.address -> store1.ref,
    n2.ref.path.address -> store2.ref, 
    n2.ref.path.address -> store3.ref)
  
  override def get(node: Node, path: String): Either[ActorRef, ActorSelection] = Left(stores.get(node).get)  
  override def put(n: (Node, String), actor: ActorRef): Unit = ???
  override def remove(node: (Node, String)): Unit = ???
}
