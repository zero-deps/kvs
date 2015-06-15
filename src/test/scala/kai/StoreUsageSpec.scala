package kai

import java.security.MessageDigest

import akka.actor.{Props, ActorSystem}
import akka.cluster.VectorClock
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import mws.rng._
import mws.rng.{StoreDelete, StorePut, StoreGet, Store}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
 *
 * Created by oleksandr
 */
class StoreUsageSpec extends TestKit(ActorSystem("RingSystemTest", ConfigFactory.parseString(StoreUsageSpec.config)))
with DefaultTimeout with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {

  val store = system.actorOf(Props(classOf[Store]), "store")
  val digester = MessageDigest.getInstance("MD5")

  val data: Data = new Data("key1", 10, 777, new VectorClock(), "value")
  val data2: Data = new Data("key3", 5, 777, new VectorClock(),  "value")
  val dataForConflict: Data  = new Data("key7", 11, 717, new VectorClock(), "value")

  "Store " must {

    "provide crud" in {
      store ! StorePut(data)

      receiveN(1) match {
        case str =>
          println(s"[PUT]$str")
      }

      store ! StoreGet(data.key)
      expectMsgType[List[Data]] should (have size 1 and contain(data))

      store ! StoreGet("not_exists")
      expectMsgType[List[Data]] should (have size 0)

      store ! StoreDelete(data.key)
      expectMsgType[String]

      store ! StoreGet(data.key)
      expectMsgType[List[Data]] should (have size 0)
    }

    "substitute old data with new" in {

      val vecktorClock = data.vc.:+("node")

      store ! StorePut(data.copy(vc = vecktorClock))
      receiveN(1) contains "ok"

      val newVc = vecktorClock.:+("node")
      store ! StorePut(data.copy(vc = newVc))
      receiveN(1) contains "ok"

      store ! StoreGet(data.key)
      receiveN(1) head match {
        case l: List[Data] => assert(l.head.vc == newVc)
        case e => fail(s"Old version is not substituted with new. res : $e")
      }

      store ! StoreDelete(data.key)
      receiveN(1) contains("ok")
    }

    "resolve conflict" in {
      val vc = dataForConflict.vc .:+("node1")

      store ! StorePut(dataForConflict.copy(vc = vc))
      receiveN(1) match {
        case str =>
          println(s"[PUT]$str")
      }

      val paralelVc = dataForConflict.vc.:+("node2")
      store ! StorePut(dataForConflict.copy(vc = paralelVc))
      receiveN(1) match {
        case str =>
          println(s"[PUT]$str")
      }

      store ! StoreGet(dataForConflict.key)
      expectMsgType[List[Data]] should ( have size 2)

      val mergedVc = vc.merge(paralelVc)
      store ! StorePut(dataForConflict.copy(vc = mergedVc))
      receiveN(1) match {
        case str =>
          println(s"[PUT]$str")
      }

      store ! StoreGet(dataForConflict.key)
      expectMsgType[List[Data]] should ( have size 1)

      store ! StoreDelete(dataForConflict.key)
      receiveN(1) match {
        case str =>
          println(s"[DELETE]$str")
      }
    }


  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}

object StoreUsageSpec {
  val config =
    """akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
      |akka.loglevel = "OFF"
      |akka.stdout-logLevel= "OFF"
      |akka.remote.log-remote-lifecycle-events = off
      |akka.log-dead-letters-during-shutdown = off
      |akka.cluster.roles = [compute]
      |akka.cluster.metrics.enabled = off
      |akka.cluster.metrics.collector-class = akka.cluster.JmxMetricsCollector
      |akka.cluster.log-info = off
      |akka.event-handlers = ["akka.event.Logging$DefaultLogger"]
      |akka.debug.receive = off
      |kai.leveldb.native=false
      |kai.leveldb.dir="store_usage_data"
      |kai.leveldb.checksum=true
      |kai.leveldb.fsync=false
    """.stripMargin
}
