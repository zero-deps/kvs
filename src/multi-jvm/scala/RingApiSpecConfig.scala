package mws.rng

import java.io.File

import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberExited, MemberRemoved, MemberUp}
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.iq80.leveldb.util.FileUtils
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import scala.concurrent.Await
import scala.concurrent.duration._

object RingSpecConfig extends MultiNodeConfig {
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  implicit val timeout = Timeout(5 seconds)

  debugConfig(on = true)
  testTransport(on = true)

  nodeConfig(node1)(ConfigFactory.parseString( """|ring.leveldb.dir=data1""".stripMargin))
  nodeConfig(node2)(ConfigFactory.parseString( """|ring.leveldb.dir=data2""".stripMargin))
  nodeConfig(node3)(ConfigFactory.parseString( """|ring.leveldb.dir=data3""".stripMargin))

  commonConfig(ConfigFactory.parseString( """
                                            |akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
                                            |akka.jvm-exit-on-fatal-error = false
                                            |akka.loglevel = "OFF"
                                            |akka.stdout-logLevel= "INFO"
                                            |akka.remote.log-remote-lifecycle-events = off
                                            |akka.log-dead-letters-during-shutdown = off
                                            |akka.cluster.roles = [ring_node]
                                            |akka.cluster.metrics.enabled = off
                                            |akka.cluster.metrics.collector-class = akka.cluster.JmxMetricsCollector
                                            |akka.cluster.log-info = off
                                            |akka.cluster.auto-down-unreachable-after = 1s
                                            |akka.event-handlers = ["akka.event.Logging$DefaultLogger"]
                                            |akka.debug.receive = off
                                            |ring.buckets = 1024
                                            |ring.virtual-nodes = 128
                                            |ring.quorum = [2,2,1]
                                            |ring.leveldb.native=true
                                            |ring.leveldb.dir="data"
                                            |ring.leveldb.checksum=true
                                            |ring.leveldb.fsync=false
                                          """.stripMargin))
}

class RingSpecMultiJvmApiNode1 extends RingApiNodeSpec
class RingSpecMultiJvmApiNode2 extends RingApiNodeSpec
class RingSpecMultiJvmApiNode3 extends RingApiNodeSpec

object RingApiNodeSpec {}

abstract class RingApiNodeSpec extends MultiNodeSpec(RingSpecConfig)
with WordSpecLike
with Matchers
with BeforeAndAfterAll
with ImplicitSender {

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = {
    multiNodeSpecAfterAll()
    FileUtils.deleteRecursively(new File("./data1"))
    FileUtils.deleteRecursively(new File("./data2"))
    FileUtils.deleteRecursively(new File("./data3"))
  }

  import RingSpecConfig._

  override def initialParticipants = roles.size

  val firstAddress = node(node1).address
  val secondAddress = node(node2).address
  val thirdAddress = node(node3).address

  "Ring extension " must {
    "gather nodes in ring" when {
      "join cluster" in within(10 seconds) {
        val cluster = Cluster(system)
        cluster.subscribe(testActor, classOf[MemberUp], classOf[MemberRemoved], classOf[MemberExited])
        expectMsgClass(classOf[CurrentClusterState])
        cluster joinSeedNodes scala.collection.immutable.Seq(firstAddress, secondAddress, thirdAddress)
        receiveN(3).collect { case MemberUp(m) => m.address}.toSet should be(Set(firstAddress, secondAddress, thirdAddress))

        testConductor.enter("all nodes joined")
        HashRing(system) // join ring
        testConductor.enter("join")
      }
    }

    "provide consistent data" when {
      "data put to one node" in within(5 seconds) {
        runOn(node1) {
          val f = HashRing(system).put("key", "val")
          val ack = Await.result(f, 3 seconds)
          ack match {
            case AckSuccess =>
            case _ => fail("ack of put operation unsuccessful")
          }
        }
        enterBarrier("put")
        runOn(node2, node3) {
          val rez = Await.result(HashRing(system).get("key"), 1 seconds)
          assertResult(Some("val"))(rez)
        }
      }

      "data updated on any node" in {
        enterBarrier("put")
        runOn(node2) {
          val f = HashRing(system).put("key", "updVal")
          val ack = Await.result(f, 3 seconds)
          ack match {
            case AckSuccess =>
            case _ => fail("Ack from put operation unsuccessful")
          }
        }
        testConductor.enter("upd")
        runOn(node1, node3) {
          awaitCond(Await.result(HashRing(system).get("key"), 1 seconds) == Some("updVal"))
        }
      }

      "data removed from any node" in {
        runOn(node2) {
          val rez = Await.result(HashRing(system).delete("key"), 1 seconds)
          assertResult(AckSuccess)(rez)
        }
        enterBarrier("delete")
        runOn(node1, node3) {
          val rez = Await.result(HashRing(system).get("key"), 1 seconds)
          assertResult(None)(rez)
        }
      }
      
      "node get down data is still available" in within(10 seconds){
        runOn(node3){
          val f = HashRing(system).put("key3", "val3")
          val ack = Await.result(f, 3 seconds)
          ack match {
            case AckSuccess =>
            case _ => fail("Ack from put operation unsuccessful")
          }
          Cluster(system).leave(thirdAddress)
        }
        enterBarrier("shutDown")
        runOn(node1,node2){
          awaitCond(Cluster(system).state.members.find(_.address eq thirdAddress) eq None, Duration(5, SECONDS))
          awaitCond(Await.result(HashRing(system).get("key3"), 3 seconds) == Some("val3"),Duration(5, SECONDS))
        }
      }
    }
  }
  
  testConductor.enter("end of test")
  Cluster(system).unsubscribe(testActor)
}
