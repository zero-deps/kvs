//package mws.rng
//
//import java.util.concurrent.TimeUnit
//import akka.actor._
//import akka.cluster.VectorClock
//import akka.testkit.{TestProbe, TestFSMRef}
//import akka.util.ByteString
//import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
//import scala.concurrent.duration.Duration
//
//
//class GatherGetSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
//  import GatherGetSpec._
//
//  "Get gather" must {
//    "process consistent data" in {
//      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, timeout, refMem))
//
//      fsm.tell(GetResp(Some(List(data))), store1.ref)
//      assertResult(Collecting)(fsm.stateName)
//      assertResult(DataCollection(List((Some(data), store1.ref.path.address)), Nil))(fsm.stateData)
//      assertResult(true)(fsm.isTimerActive("send_by_timeout"))
//
//      fsm.tell(GetResp(Some(List(data))), store2.ref)
//      assertResult(Sent)(fsm.stateName)
//      assertResult(DataCollection(List(
//        (Some(data), store1.ref.path.address),
//        (Some(data), store2.ref.path.address)), Nil))(fsm.stateData)
//
//      client.expectMsg(Some(ByteString("value")))
//      assertResult(true)(fsm.isTimerActive("send_by_timeout"))
//
//      fsm.tell(GetResp(Some(List(data))), store3.ref) // send third to finish process
//    }
//
//    "return empty value" in {
//      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, timeout, refMem))
//
//      fsm.tell(GetResp(None), store1.ref)
//      assertResult(Collecting)(fsm.stateName)
//      assertResult(DataCollection(List((None, store1.ref.path.address)), Nil))(fsm.stateData)
//      assertResult(true)(fsm.isTimerActive("send_by_timeout"))
//
//      fsm.tell(GetResp(None), store2.ref)
//      assertResult(Sent)(fsm.stateName)
//      assertResult(DataCollection(List(
//        (None, store1.ref.path.address),
//        (None, store2.ref.path.address)), Nil))(fsm.stateData)
//      assertResult(true)(fsm.isTimerActive("send_by_timeout"))
//
//      client.expectMsg(None)
//
//      fsm.tell(GetResp(None), store3.ref) // send third to finish process
//    }
//
//    "merge sequential history" in {
//      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, timeout, refMem))
//
//      val updated: Data = data.copy(vc = data.vc.:+("node1"), value = ByteString("newvalue"))
//      fsm.tell(GetResp(Some(List(updated))), store1.ref)
//      assertResult(Collecting)(fsm.stateName)
//      assertResult(DataCollection(List((Some(updated), store1.ref.path.address)), Nil))(fsm.stateData)
//
//      fsm.tell(GetResp(Some(List(data))), store2.ref)
//      assertResult(Sent)(fsm.stateName)
//      assertResult(DataCollection(List(
//        (Some(data), store2.ref.path.address),
//        (Some(updated), store1.ref.path.address)), Nil))(fsm.stateData)
//
//      client.expectMsg(Some(ByteString("newvalue")))
//      fsm.tell(GetResp(None), store3.ref) // send third to finish process
//
//      store3.expectMsg(StorePut(updated)) //TODO TestProbe path.address == other stores. last store will be resolved from ActorRefStorage.Fix
//
//    }
//
//    "merge conflict vector clock by last modified" in {
//      val conflictData = data.copy(vc = data.vc.:+("node2"), value = ByteString("another_val"), lastModified = data.lastModified + 1)
//      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, timeout, refMem))
//
//      fsm.tell(GetResp(Some(List(data))), store1.ref)
//      assertResult(Collecting)(fsm.stateName)
//      assertResult(DataCollection(List((Some(data), store1.ref.path.address)), Nil))(fsm.stateData)
//      assertResult(true)(fsm.isTimerActive("send_by_timeout"))
//
//      fsm.tell(GetResp(Some(List(conflictData))), store2.ref)
//      assertResult(Sent)(fsm.stateName)
//
//      assertResult(DataCollection(List(
//        (Some(conflictData), store2.ref.path.address),
//        (Some(data), store1.ref.path.address)),
//        List()))(fsm.stateData)
//
//      assertResult(true)(fsm.isTimerActive("send_by_timeout"))
//
//      client.expectMsg(Some(conflictData.value))
//      fsm.tell(GetResp(Some(List(data))), store3.ref) // send 3-rd
//      store3.expectMsg(StorePut(conflictData))
//    }
//
//    "send by timeout" in {
//      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, timeout, refMem))
//
//      fsm.tell(GetResp(Some(List(data))), store1.ref)
//      assertResult(Collecting)(fsm.stateName)
//      assertResult(DataCollection(List((Some(data), store1.ref.path.address)), Nil))(fsm.stateData)
//      assertResult(true)(fsm.isTimerActive("send_by_timeout"))
//
//      client.receiveOne(Duration(timeout + 1, TimeUnit.SECONDS)) should be(Some(data.value))
//    }
//
//    "send None by timeout if no one store answer" in {
//      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, timeout, refMem))
//      client.receiveOne(Duration(timeout + 1, TimeUnit.SECONDS)) should be(None)
//    }
//
//    "merge inconsistent data" in {
//      val data2 = data.copy(vc = data.vc.:+("node2"), lastModified = data.lastModified + 100, value = ByteString("upd"))
//
//      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, timeout, refMem))
//
//      fsm.tell(GetResp(Some(List(data, data2))), store1.ref)
//      assertResult(Collecting)(fsm.stateName)
//      assertResult(DataCollection(Nil, List((List(data, data2), store1.ref.path.address))))(fsm.stateData)
//      assertResult(true)(fsm.isTimerActive("send_by_timeout"))
//
//      fsm.tell(GetResp(Some(List(data))), store2.ref)
//      assertResult(Sent)(fsm.stateName)
//      assertResult(DataCollection(List((Some(data), store2.ref.path.address)),
//        List((List(data, data2), store1.ref.path.address))))(fsm.stateData)
//      assertResult(true)(fsm.isTimerActive("send_by_timeout"))
//
//      client.expectMsg(Some(data2.value))
//      fsm.tell(GetResp(Some(List(data))), store3.ref)
//      store3.expectMsg(StorePut(data2))
//    }
//  }
//
//  override def afterAll() {
//    system.shutdown()
//  }
//}
//
//object GatherGetSpec {
//  implicit val system = ActorSystem()
//
//  val client = new TestProbe(system)
//  val store1 = new TestProbe(system)
//  val store2 = new TestProbe(system)
//  val store3 = new TestProbe(system)
//
//  var refMem: ActorRefStorage = new Mem
//
//  val data = new Data("key", 123, 999999, new VectorClock().:+("node1"), ByteString("value"))
//  val N = 3
//  val R = 2
//  val timeout = 1
//}
//
//class Mem extends ActorRefStorage {
//
//  import GatherGetSpec._
//
//  val stores = Map(
//    store1.ref.path.address -> store1.ref,
//    store2.ref.path.address -> store2.ref,
//    store3.ref.path.address -> store3.ref)
//
//  override def get(node: Node, path: String): Either[ActorRef, ActorSelection] = Left(stores.get(node).get)
//
//  override def put(n: (Node, String), actor: ActorRef): Unit = ???
//
//  override def remove(node: (Node, String)): Unit = ???
//}
