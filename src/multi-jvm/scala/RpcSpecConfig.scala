package mws.rng

import java.security.MessageDigest

import akka.actor.Props
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberExited, MemberRemoved, MemberUp}
import akka.cluster.{Cluster, VectorClock}
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import mws.rng.{StorePut, Store, Hash}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

object KaiSpecConfig extends MultiNodeConfig{
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  implicit val timeout = Timeout(5 seconds)

  debugConfig(on = true)
  testTransport(on = true)

  nodeConfig(node1)(ConfigFactory.parseString("""|kai.leveldb.dir=data1""".stripMargin))
  nodeConfig(node2)(ConfigFactory.parseString("""|kai.leveldb.dir=data2""".stripMargin))
  nodeConfig(node3)(ConfigFactory.parseString("""|kai.leveldb.dir=data3""".stripMargin))

  commonConfig(ConfigFactory.parseString("""
    |akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    |akka.jvm-exit-on-fatal-error = false
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
    |kai.sync_interval = 1000
    |kai.buckets = 1024
    |kai.virtual-nodes = 128
    |kai.quorum = [3,2,2]
    |kai.leveldb.native=true
    |kai.leveldb.dir="data"
    |kai.leveldb.checksum=true
    |kai.leveldb.fsync=false
    """.stripMargin))
}

class KaiSpecMultiJvmNode1 extends KaiNodeSpec
class KaiSpecMultiJvmNode2 extends KaiNodeSpec
class KaiSpecMultiJvmNode3 extends KaiNodeSpec

object KaiNodeSpec {}

abstract class KaiNodeSpec extends MultiNodeSpec(KaiSpecConfig)
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ImplicitSender{

  override def beforeAll() = multiNodeSpecBeforeAll()
  override def afterAll() = multiNodeSpecAfterAll()

  import mws.rng.KaiSpecConfig._

  override def initialParticipants = roles.size

  val firstAddress = node(node1).address
  val secondAddress = node(node2).address
  val thirdAddress = node(node3).address

  private val hash = system.actorOf(Props[Hash], name="kai_hash")
  private val store= system.actorOf(Props[Store], name="kai_store")
  lazy val config = system.settings.config.getConfig("kai")

  val digester = MessageDigest.getInstance("MD5")
  val dataForImplicitSynch: Data = new Data("key1", 2, 777, new VectorClock(), digester.digest("value".getBytes).mkString, "0", "value")
  val dataForTimeoutSynch: Data = new Data("key2", 100, 888, new VectorClock(), digester.digest("value2".getBytes).mkString, "0", "value2")
  
  "start all nodes in cluster" in within(10 seconds) {
    Cluster(system).subscribe(testActor, classOf[MemberUp], classOf[MemberRemoved], classOf[MemberExited])
    expectMsgClass(classOf[CurrentClusterState])
    Cluster(system) joinSeedNodes scala.collection.immutable.Seq(firstAddress, secondAddress, thirdAddress)

    receiveN(3).collect { case MemberUp(m) => m.address }.toSet should be(
      Set(firstAddress, secondAddress, thirdAddress))
    
    
  }

  "sample must" must {
    enterBarrier("startup")

    "wait for all nodes to startup" in { enterBarrier("startup") }

    "communicate between remote nodes" when {
      runOn(node1) {
        s"respond with (${firstAddress.port}) on NodeList" in {
          Thread.sleep(200) //wait until membership processes complited
          
          //cluster.state.members
          
          //hash ! NodeList
          //expectMsgType[List[Address]] should (have size 3 and contain(firstAddress))
        }
        enterBarrier("deployed")
      }

      runOn(node2) {
        s"respond with (${secondAddress.port}) on NodeList" in {
          Thread.sleep(200) //wait until membership processes complited
          //hash ! NodeList
          //expectMsgType[List[Address]] should (have size 3 and contain (secondAddress))
        }
        enterBarrier("deployed")
      }

      runOn(node3){
        s"respond with (${thirdAddress.port}) on NodeList" in {
          Thread.sleep(200) //wait until membership processes complits
          //hash ! NodeList
          //expectMsgType[List[Address]] should (have size 3 and contain (thirdAddress))
        }
        enterBarrier("deployed")
      }

      "wait all nodes to selfcheck" in {enterBarrier("check")}

      //sync explicitly
      runOn(node2){

        store ! StorePut(dataForImplicitSynch)
        receiveN(1) foreach(str => println(s"$str"))

        store ! StoreBucketValue(dataForImplicitSynch._2)
        expectMsgType[List[Data]] should( have size 1)

      }

      runOn(node1){
        s" at node (${firstAddress.port}) get synched data from (${secondAddress.port})" in {
//          hash ! NodeList

//          expectMsgType[List[Address]](10 seconds) should
//          (have size 3 and contain(thirdAddress) and contain(secondAddress) and contain(firstAddress))


//          hash ! UpdateBucket(dataForImplicitSynch._2)

          Thread.sleep(1000)

          store ! StoreBucketValue(dataForImplicitSynch._2)
          expectMsgType[List[Data]] should( have size 1)

        }
      }
      "end of implicit synch barrier" in {enterBarrier("implicit sync barrier")}
      runOn(node1){
        s"respond  on NodeList with node checked by timeout" in {
//          hash ! NodeList
//          expectMsgType[List[Address]] should (have size 3 and contain (thirdAddress))
        }
      }
      
    }

    "finished barrier " in {  enterBarrier("finished")}

    Cluster(system).unsubscribe(testActor)
    testConductor.enter("all-up")
  }

}
