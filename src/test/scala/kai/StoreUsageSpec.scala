package kai

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.cluster.VectorClock
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import mws.rng.{Store, _}
import org.iq80.leveldb.util.FileUtils
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration.Duration

/**
 *
 * Created by oleksandr
 */
class StoreUsageSpec extends TestKit(ActorSystem("RingSystemTest", ConfigFactory.parseString(StoreUsageSpec.config)))
with DefaultTimeout with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {

  val store = system.actorOf(Props(classOf[Store]), "store")
  
  val data: Data = new Data("key1", 10, 777, new VectorClock(), ByteString( "value"))

  "Store " must {

    "provide crud" in {
      store ! StorePut(data)
      expectMsgType[String] should equal("ok")

      store ! StoreGet(data.key)
      expectMsgType[Option[List[Data]]] should equal(Some(List(data)))

      store ! StoreGet("not_exists")
      expectMsgType[Option[List[Data]]] should equal(None)

      store ! StoreDelete(data.key)
      expectMsgType[String] should equal("ok")

      store ! StoreGet(data.key)
      expectMsgType[Option[Data]] should equal(None)
    }

    "substitute update old version with new" in {
      val vectorClock = data.vc.:+("node1")

      store ! StorePut(data.copy(vc = vectorClock))
      expectMsgType[String] should equal("ok")

      val newVc = vectorClock.:+("node2")
      store ! StorePut(data.copy(vc = newVc))
      expectMsgType[String] should equal("ok")

      store ! StoreGet(data.key)
      receiveN(1) head match {
        case updData: Some[List[Data]] => updData.get.foreach(d => assert(d.vc == newVc))
        case e => fail(s"Old version is not substituted with new. res : $e")
      }

      store ! StoreDelete(data.key)
      expectMsgType[String] should equal("ok")
    }

    "save concurrent data and substitute with merged" in {
      val vc1 = data.vc.:+("node1")
      val vc2 = data.vc.:+("node2")

      store ! StorePut(data.copy(vc = vc1))
      expectMsgType[String] should equal("ok")

      store ! StorePut(data.copy(vc = vc2))
      expectMsgType[String] should equal("ok")

      store ! StoreGet(data.key)
      receiveOne(Duration(3, TimeUnit.SECONDS)) match {
          
        case dataList: Some[List[Data]] =>
          val vectorClocks = dataList.get.map(d => d.vc)
          assert(vectorClocks.size == 2)
          assert(vectorClocks.contains(vc1))
          assert(vectorClocks.contains(vc2))

        case e => fail(s"Concurrent data not persisted, Rez = $e")
      }
      
      val mergeVersion = vc1.merge(vc2)
      store ! StorePut(data.copy(vc = mergeVersion))
      expectMsgType[String] should equal("ok")

      store ! StoreGet(data.key)
      receiveOne(Duration(3, TimeUnit.SECONDS)) match {
        case dataList: Some[List[Data]] =>
          val vectorClocks = dataList.get.map(d => d.vc)
          assert(vectorClocks.size == 1)
          assert(vectorClocks.contains(mergeVersion))

        case e => fail(s"Concurrent data not persisted, Rez = $e")
      }

    } 

  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    FileUtils.deleteRecursively(new File("./store_usage_data"))
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
      |ring.leveldb.native=false
      |ring.leveldb.dir="store_usage_data"
      |ring.leveldb.checksum=true
      |ring.leveldb.fsync=false
    """.stripMargin
}
