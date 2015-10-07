package kai

import java.io.File
import java.util.concurrent.TimeUnit
import akka.actor.{ActorSystem, Props}
import akka.cluster.VectorClock
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import mws.rng._
import org.iq80.leveldb.impl.Iq80DBFactory
import org.iq80.leveldb.{CompressionType, WriteOptions, ReadOptions, Options}
import org.iq80.leveldb.util.FileUtils
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import scala.concurrent.duration.Duration

object StoreUsageSpec {
  val leveldbOptions = new Options().createIfMissing(true)
  def leveldbReadOptions = new ReadOptions().verifyChecksums(false)
  val leveldbWriteOptions = new WriteOptions().sync(false).snapshot(false)
  val leveldbDir = new File("store_spec")
  val bucketsNum = 1024
  var leveldb = Iq80DBFactory.factory.open(leveldbDir, leveldbOptions.compressionType(CompressionType.NONE))
  val conf =
    """ ring.leveldb {
      |fsync=false
      |checksum=falase
      |}
    """.stripMargin
}

class StoreUsageSpec extends TestKit(ActorSystem("RingSystemTest",ConfigFactory.parseString(StoreUsageSpec.conf)))
with DefaultTimeout with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {
  import StoreUsageSpec._

  val writeStore = system.actorOf(Props(new WriteStore(leveldb)))
  val readStore = system.actorOf(Props(new ReadonlyStore(leveldb)))
  
  val data: Data = new Data("key1", 10, 777, new VectorClock(), ByteString( "value"))

  "Store " must {

    "provide crud" in {
      writeStore ! StorePut(data)
      expectMsgType[PutStatus] should equal(Saved)

      readStore ! StoreGet(data.key)
      expectMsgType[GetResp] should equal(GetResp(Some(List(data))))

      readStore ! StoreGet("not_exists")
      expectMsgType[GetResp] should equal(GetResp(None))

      writeStore ! StoreDelete(data.key)
      expectMsgType[String] should equal("ok")

      readStore ! StoreGet(data.key)
      expectMsgType[GetResp] should equal(GetResp(None))
    }

    "substitute update old version with new" in {
      val vectorClock = data.vc.:+("node1")

      writeStore ! StorePut(data.copy(vc = vectorClock))
      expectMsgType[PutStatus] should equal(Saved)

      val newVc = vectorClock.:+("node2")
      writeStore ! StorePut(data.copy(vc = newVc))
      expectMsgType[PutStatus] should equal(Saved)

      readStore ! StoreGet(data.key)
      receiveN(1).head match {
        case GetResp(Some(updData)) if updData.size == 1 => updData.foreach(d => assert(d.vc == newVc))        
        case e => fail(s"Old version is not substituted with new. res : $e")
      }

      writeStore ! StoreDelete(data.key)
      expectMsgType[String] should equal("ok")

      readStore ! StoreGet(data.key)
      expectMsgType[GetResp] should equal(GetResp(None))
    }


    "save concurrent data and substitute with merged" in {
      val vc1 = data.vc.:+("node1")
      val vc2 = data.vc.:+("node2")

      writeStore ! StorePut(data.copy(vc = vc1))
      expectMsgType[PutStatus] should equal(Saved)

      writeStore ! StorePut(data.copy(vc = vc2))
      expectMsgType[PutStatus] should equal(Conflict(List(data.copy(vc = vc1))))

      readStore ! StoreGet(data.key)
      receiveOne(Duration(3, TimeUnit.SECONDS)) match {
        case GetResp(dataList) if dataList.get.size == 2 =>
          val vectorClocks = dataList.get.map(_.vc)
          assertResult(Set(vc1,vc2))(vectorClocks.toSet)

        case incorrect => fail(s"Concurrent vector clock not persisted, Rez = $incorrect")
      }

//      val mergeVersion = vc1.merge(vc2)
//      writeStore ! StorePut(data.copy(vc = mergeVersion))
//      expectMsgType[PutStatus] should equal(Saved)
//
//      readStore ! StoreGet(data.key)
//      receiveOne(Duration(3, TimeUnit.SECONDS)) match {
//        case GetResp(dataList)=>
//          val vectorClocks = dataList.get.map(d => d.vc)
//          assert(vectorClocks.size == 1)
//          assert(vectorClocks.contains(mergeVersion))
//
//        case e => fail(s"Concurrent data not persisted, Rez = $e")
//      }

    }
  }

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
    FileUtils.deleteRecursively(new File("./store_spec"))
  }
}
