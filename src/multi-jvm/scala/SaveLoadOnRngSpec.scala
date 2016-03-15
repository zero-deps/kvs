package mws.rng

import akka.remote.testkit.{MultiNodeSpec, MultiNodeSpecCallbacks, MultiNodeConfig}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import com.typesafe.config.ConfigFactory
import akka.cluster.Cluster
import akka.util.ByteString
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberUp
import scala.concurrent.Await
import org.iq80.leveldb.util.FileUtils
import java.io.File
import scala.concurrent.duration._


object LoadSaveConfig extends MultiNodeConfig {
	val common_config = ConfigFactory.parseString("""
		|akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
	    |akka.remote.log-remote-lifecycle-events = off
	    |akka.log-dead-letters = off
	    |akka.loglevel = "INFO"
		|akka.stdout-logLevel= "INFO"
	    |akka.log-dead-letters-during-shutdown = off
	    |akka.testconductor.barrier-timeout = 20s
	    |akka.cluster.log-info = on
	    |akka.test.timefactor = 4
	    |akka.actor.debug.receive = off
	    |akka.cluster.failure-detector.threshold = 8
	    |akka.cluster.metrics.collector-class = akka.cluster.JmxMetricsCollector
	    |ring.buckets = 32
		|ring.virtual-nodes = 8
        |ring.quorum = [2,2,1]""".stripMargin) 

	commonConfig(common_config)
	val n1 = role("n1_for_save")
	val n2 = role("n2_for_save")
	val n3 = role("n3_for_load")
	val n4 = role("n4_for_load")	

	nodeConfig(n1)(ConfigFactory.parseString( """|ring.leveldb.dir=data1""".stripMargin))
  	nodeConfig(n2)(ConfigFactory.parseString( """|ring.leveldb.dir=data2""".stripMargin))
  	nodeConfig(n3)(ConfigFactory.parseString( """|ring.leveldb.dir=data3""".stripMargin))
  	nodeConfig(n4)(ConfigFactory.parseString( """|ring.leveldb.dir=data4""".stripMargin))
}

class LoadSaveSpecMultiJvmNode1 extends LoadSaveSpec
class LoadSaveSpecMultiJvmNode2 extends LoadSaveSpec
class LoadSaveSpecMultiJvmNode3 extends LoadSaveSpec
class LoadSaveSpecMultiJvmNode4 extends LoadSaveSpec

class LoadSaveSpec extends STMultiNodeSpecTraits(LoadSaveConfig) {
	import LoadSaveConfig._
	"Save then Load Spec" must {
		Cluster(system).subscribe(testActor, classOf[MemberUp])
		expectMsgClass(classOf[CurrentClusterState])


		"populate n1 n2 nodes with data" in {
			runOn(n1,n2) {
				Cluster(system).join(node(n1).address)
				 awaitCond(Cluster(system).state.members.size == 2)
				 awaitCond(Await.result(HashRing(system).isReady, 3.second))
				 
				 (1 to 100).foreach{ v => 
				 	HashRing(system).put(s"$v", ByteString(s"$v"))
				 	awaitCond(Await.result(HashRing(system).get(s"$v"), 3.second) ==  Some(ByteString(s"$v")), 3.second)
				 }
			}
			enterBarrier("first_nodes_populated_with_data")	
		}

		"dump data on first node" in {
			runOn(n1) {
				HashRing(system).dump()

			}
			enterBarrier("all_waiting_for_dump")	
		}

		"join n3 n4 in ring " in {
			runOn(n3,n4){
				Cluster(system).join(node(n3).address)
				awaitCond(Cluster(system).state.members.size == 2)
				awaitCond(Await.result(HashRing(system).isReady, 3.second))
			}
			enterBarrier("second_cluster_initialised")	
		}

		"load data and check" in {
			runOn(n3){
				HashRing(system).load("rng_dump_2016.03.15-16.03.28.zip") // tmp hack. 
				Thread.sleep(5000)
			}
			runOn(n3,n4){
				(1 to 100).foreach{ v => 
				 	HashRing(system).put(s"$v", ByteString(s"$v"))
				 	awaitCond(Await.result(HashRing(system).get(s"$v"), 3.second) ==  Some(ByteString(s"$v")), 3.second)
				 }
			}
		}

		"finish tests" in {
			enterBarrier("FIN")
		}
	}
}

class STMultiNodeSpecTraits(c: MultiNodeConfig) extends MultiNodeSpec(c) with MultiNodeSpecCallbacks with Matchers 
									with WordSpecLike with BeforeAndAfterAll {

	override def initialParticipants = roles.size										
	override def beforeAll() = multiNodeSpecBeforeAll()
	override def afterAll() = {
		multiNodeSpecAfterAll()
		(1 to 4).foreach{n => FileUtils.deleteRecursively(new File(s"./data$n"))}
	}
}