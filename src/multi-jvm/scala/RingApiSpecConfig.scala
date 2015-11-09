package mws.rng

import java.io.File

import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberExited, MemberRemoved, MemberUp}
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import akka.util.{ByteString, Timeout}
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
  //  testTransport(on = true)

  nodeConfig(node1)(ConfigFactory.parseString( """|ring.leveldb.dir=data1""".stripMargin))
  nodeConfig(node2)(ConfigFactory.parseString( """|ring.leveldb.dir=data2""".stripMargin))
  nodeConfig(node3)(ConfigFactory.parseString( """|ring.leveldb.dir=data3""".stripMargin))

  commonConfig(ConfigFactory.parseString( """
                                            |akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
                                            |akka.jvm-exit-on-fatal-error = false
                                            |akka.loglevel = "INFO"
                                            |akka.stdout-logLevel= "INFO"
                                            |akka.remote.log-remote-lifecycle-events = off
                                            |akka.log-dead-letters-during-shutdown = off
                                            |akka.cluster.metrics.enabled = off
                                            |akka.cluster.seed-nodes = []
                                            |akka.cluster.metrics.collector-class = akka.cluster.JmxMetricsCollector
                                            |akka.cluster.auto-down-unreachable-after = off
                                            |akka.event-handlers = ["akka.event.Logging$DefaultLogger"]
                                            |akka.debug.receive = off
                                            |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
                                            |enabled-transports = ["akka.remote.netty.tcp"]
                                            |ring.buckets = 1024
                                            |ring.gather-timeout = 3
                                            |ring.virtual-nodes = 128
                                            |ring.quorum = [2,2,1]
                                            |ring.leveldb.native=true
                                            |ring.leveldb.dir="data"
                                            |ring.leveldb.checksum=true
                                            |ring.leveldb.fsync=false
                                          """.stripMargin))

  def bstr(str: String) = ByteString(str)
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
    "gather nodes in ring" in within(20.seconds) {
      val cluster = Cluster(system)

      cluster joinSeedNodes scala.collection.immutable.Seq(firstAddress, secondAddress, thirdAddress)

      awaitCond(cluster.state.members.size == 3)
      // join ring
      testConductor.enter("join-entire-cluster")
      runOn(node1, node2, node3) {
        awaitCond(Await.result(HashRing(system).isReady, 3.second))
      }
      testConductor.enter("join-ring")
      runOn(node1) {
        log.info(s"PUT on node 1 ")
        awaitCond(Await.result(HashRing(system).put("key", bstr("val")), 5.seconds) == AckSuccess)
      }      
      enterBarrier("put-on-first-node")
      
      runOn(node2, node3) {
        val rez = Await.result(HashRing(system).get("key"), 3.seconds)
        assertResult(Some(ByteString("val")))(rez)
      }
      testConductor.enter("end of test")
    }
  }
}
