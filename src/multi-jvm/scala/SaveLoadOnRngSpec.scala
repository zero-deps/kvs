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
	    |akka.loglevel = "INFO"
		|akka.stdout-logLevel= "INFO"
	    |akka.log-dead-letters-during-shutdown = off
	    |akka.testconductor.barrier-timeout = 40s
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

	var dump_file_location: Option[String] = None

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
  import DumpFileUtils._
	"Save then Load Spec" must {
		Cluster(system).subscribe(testActor, classOf[MemberUp])
		expectMsgClass(classOf[CurrentClusterState])

		"populate n1 n2 nodes with data" in {
			runOn(n1,n2) {
				Cluster(system).join(node(n1).address)
				 awaitCond(Cluster(system).state.members.size == 2)
				 awaitCond(Await.result(HashRing(system).isReady, 10.second))
				 
			}
			enterBarrier("n1 n2 are ready")
			runOn(n1){
				 (1 to 50).foreach{ v =>
				 	HashRing(system).put(s"$v", ByteString(s"$v"))
				 	awaitCond(Await.result(HashRing(system).get(s"$v"), 5.second) ==  Some(ByteString(s"$v")), 5.second)
				 }
			}
			
			runOn(n2){	
			(51 to 100).foreach{ v =>
				HashRing(system).put(s"$v", ByteString(s"$v"))
				awaitCond(Await.result(HashRing(system).get(s"$v"), 5.second) ==  Some(ByteString(s"$v")), 5.second)}
			}
			enterBarrier("n1 n2 are filled with data")
		
			runOn(n1) {
				dump_file_location = Some(Await.result(HashRing(system).dump(), 10.seconds))
				println(s"$dump_file_location file location")
        		assert(dump_file_location == findDumpFile)
        		enterBarrier("all_waiting_for_dump")	
			}


			runOn(n2){
				enterBarrier("all_waiting_for_dump")
			}
			runOn(n3,n4){
				enterBarrier("all_waiting_for_dump")
				 Cluster(system).join(node(n3).address)
				 awaitCond(Cluster(system).state.members.size == 2)
				 awaitCond(Await.result(HashRing(system).isReady, 10.second))
			}

			runOn(n3){
				findDumpFile match {
					case None => fail("dump not ready")
					case Some(file_location) => 
						println(s"$file_location  - dump file")
						HashRing(system).load(file_location)
				}
			}

			runOn(n3,n4){

				//awaitCond(Await.result(HashRing(system).isReady, 20.second)) // wait for load finish
				Thread.sleep(5000)
				println(s"#####READY#########")
				(1 to 100).foreach{ v =>
				 	val result: Option[Value] = Await.result(HashRing(system).get(s"$v"), 5.second)
					println(s"***********  $result")
					awaitCond(result ==  Some(ByteString(s"$v")), 5.second)
				 }
			}
			enterBarrier("FIN")
		}

	}
}

class STMultiNodeSpecTraits(c: MultiNodeConfig) extends MultiNodeSpec(c) with MultiNodeSpecCallbacks with Matchers 
									with WordSpecLike with BeforeAndAfterAll {
  import DumpFileUtils._

	override def initialParticipants = roles.size										
	override def beforeAll() = {
    multiNodeSpecBeforeAll()
    allDumpStaff.foreach{f=> FileUtils.deleteRecursively(f)}
  }

	override def afterAll() = {
		multiNodeSpecAfterAll()
		(1 to 4).foreach{n => FileUtils.deleteRecursively(new File(s"./data$n"))}
    allDumpStaff.foreach{f=> FileUtils.deleteRecursively(f)}
	}
}

/*
  * Utils for find dump file. Returned value of HashRing.dump() method cannot be used in multi-jvm tests because memory
  *  is not shared accross test JVMs. Test also check whether founded file exactly the same file we got in HashRing.dump()
   */
object DumpFileUtils{
  def findDumpFile: Option[String] = {
    val currentDir = new File("./")
    currentDir.list().find(file => file.startsWith("rng_dump") && file.endsWith(".zip"))
  }

  def allDumpStaff: Array[File] = {
    val currentDir = new File("./")
    currentDir.listFiles().filter(file => file.getName.contains("rng_dump"))
  }
}