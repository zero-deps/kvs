package mws.rng

import java.util.concurrent.TimeUnit
import akka.actor._
import akka.cluster.VectorClock
import akka.testkit.{TestProbe, TestFSMRef}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import stores.{GetResp, StorePut}
import scala.concurrent.duration.Duration


class GatherGetSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  import GatherGetSpec._

  "Get gather" must {
    "process consistent data" in {
      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, data.key))

      fsm.tell(GetResp(Some(List(data))), store1.ref)
      assertResult(Collecting)(fsm.stateName)
      assertResult(DataCollection(List((Some(data), store1.ref.path.address)), 1))(fsm.stateData)

      fsm.tell(GetResp(Some(List(data))), store2.ref)
      assertResult(Collecting)(fsm.stateName)
      assertResult(DataCollection(List(
        (Some(data), store1.ref.path.address),
        (Some(data), store2.ref.path.address)), 2))(fsm.stateData)
      assertResult(true)(fsm.isTimerActive("send_by_timeout"))

      fsm.tell(GetResp(Some(List(data))), store3.ref)

      client.expectMsg(Some(ByteString("value")))
    }

    "return empty value" in {
      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, ""))

      fsm.tell(GetResp(None), store1.ref)
      assertResult(Collecting)(fsm.stateName)
      assertResult(DataCollection(List((None, store1.ref.path.address)), 1))(fsm.stateData)
      assertResult(true)(fsm.isTimerActive("send_by_timeout"))

      fsm.tell(GetResp(None), store2.ref)
      assertResult(Collecting)(fsm.stateName)
      assertResult(DataCollection(List(
        (None, store1.ref.path.address),
        (None, store2.ref.path.address)), 2))(fsm.stateData)
      assertResult(true)(fsm.isTimerActive("send_by_timeout"))

      fsm.tell(GetResp(None), store3.ref)
      client.expectMsg(None)
    }

    "merge sequential history" in {
      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, data.key))

      val updated: Data = data.copy(vc = data.vc.:+("node1"), value = ByteString("newvalue"))
      fsm.tell(GetResp(Some(List(updated))), store1.ref)
      assertResult(Collecting)(fsm.stateName)
      assertResult(DataCollection(List((Some(updated), store1.ref.path.address)), 1))(fsm.stateData)

      fsm.tell(GetResp(Some(List(updated))), store2.ref)
      assertResult(Collecting)(fsm.stateName)
      assertResult(DataCollection(List(
        (Some(updated), store2.ref.path.address),
        (Some(updated), store1.ref.path.address)), 2))(fsm.stateData)
      fsm.tell(GetResp(Some(List(data))), store3.ref)

      client.expectMsg(Some(ByteString("newvalue")))
//      store3.expectMsg(StorePut(updated))
    }

    "merge conflict vector clock by last modified" in {
      val conflictData = data.copy(vc = data.vc.:+("node2"), value = ByteString("another_val"), lastModified = data.lastModified + 1)
      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, data.key))

      fsm.tell(GetResp(Some(List(data))), store1.ref)
      assertResult(Collecting)(fsm.stateName)
      assertResult(DataCollection(List((Some(data), store1.ref.path.address)), 1))(fsm.stateData)
      assertResult(true)(fsm.isTimerActive("send_by_timeout"))

      fsm.tell(GetResp(Some(List(conflictData))), store2.ref)
      assertResult(Collecting)(fsm.stateName)

      assertResult(DataCollection(List(
        (Some(conflictData), store2.ref.path.address),
        (Some(data), store1.ref.path.address)), 2))(fsm.stateData)

      assertResult(true)(fsm.isTimerActive("send_by_timeout"))

      fsm.tell(GetResp(Some(List(data))), store3.ref)
      client.expectMsg(Some(conflictData.value))
//      store3.expectMsg(StorePut(conflictData))
    }

    "send by timeout" in {
      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, data.key))

      fsm.tell(GetResp(Some(List(data))), store1.ref)
      assertResult(Collecting)(fsm.stateName)
      assertResult(DataCollection(List((Some(data), store1.ref.path.address)), 1))(fsm.stateData)
      assertResult(true)(fsm.isTimerActive("send_by_timeout"))

      client.receiveOne(Duration(gather_timeout + 1, TimeUnit.SECONDS)) should be(Some(data.value))
    }

    "send None by timeout if no one store answer" in {
      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R, ""))
      client.receiveOne(Duration(gather_timeout + 1, TimeUnit.SECONDS)) should be(None)
    }

    "merge inconsistent data" in {
      val data2 = data.copy(vc = data.vc.:+("node2"), lastModified = data.lastModified + 100, value = ByteString("upd"))

      val fsm = TestFSMRef(new GatherGetFsm(client.ref, N, R,data.key))

      fsm.tell(GetResp(Some(List(data, data2))), store1.ref)
      assertResult(Collecting)(fsm.stateName)
      assertResult(DataCollection(List((Some(data), store1.ref.path.address), (Some(data2), store1.ref.path.address)),1))(fsm.stateData)
      assertResult(true)(fsm.isTimerActive("send_by_timeout"))

      fsm.tell(GetResp(Some(List(data))), store2.ref)
      assertResult(Collecting)(fsm.stateName)
      assertResult(DataCollection(List((Some(data), store2.ref.path.address),
        (Some(data), store1.ref.path.address), (Some(data2), store1.ref.path.address)),2))(fsm.stateData)
      assertResult(true)(fsm.isTimerActive("send_by_timeout"))

      fsm.tell(GetResp(Some(List(data))), store3.ref)
      client.expectMsg(Some(data2.value))
//      store3.expectMsg(StorePut(data2))
    }
  }

  override def afterAll() {
    system.shutdown()
  }
}

object GatherGetSpec {
  implicit val system = ActorSystem("GatherGetSpec", ConfigFactory.parseString(
    """
    ring.gather-timeout = 2
    """.stripMargin))
  val client = new TestProbe(system)
  val store1 = new TestProbe(system)
  val store2 = new TestProbe(system)
  val store3 = new TestProbe(system)

  val data = new Data("key", 123, 999999, new VectorClock().:+("node1"), ByteString("value"))
  val N = 3
  val R = 2
  val gather_timeout = 2
}

