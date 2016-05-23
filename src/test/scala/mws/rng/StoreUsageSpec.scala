package mws.rng

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
import mws.rng.store._

import scala.concurrent.duration.Duration

object StoreUsageSpec {
  val leveldbOptions = new Options().createIfMissing(true)
  def leveldbReadOptions = new ReadOptions().verifyChecksums(false)
  val leveldbWriteOptions = new WriteOptions().sync(false).snapshot(false)
  val leveldbDir = new File("store_spec")
  var leveldb = Iq80DBFactory.factory.open(leveldbDir, leveldbOptions.compressionType(CompressionType.NONE))
  val conf =
    """
    |ring.buckets = 10
    |ring.hashLength = 32
    """.stripMargin
}

class StoreUsageSpec extends TestKit(ActorSystem("RingSystemTest", ConfigFactory.parseString(StoreUsageSpec.conf).
  withFallback(ConfigFactory.load())))
with DefaultTimeout with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {
  import StoreUsageSpec._

  val d = Duration(3, TimeUnit.SECONDS)
  val writeStore = system.actorOf(Props(new WriteStore(leveldb)))
  val readStore = system.actorOf(Props(new ReadonlyStore(leveldb)))
  val hashing = HashingExtension(system)
  
  val data: Data = new Data("key1", hashing.findBucket("313fdy1"), 777, new VectorClock(), ByteString( "value"))
  
  "Store " must {

    s"provide crud for $data " in {
      writeStore ! StorePut(data)
      expectMsg(Saved)

      readStore ! BucketKeys(data.bucket)
      expectMsg(List(data.key))

      get_delete_get(data)
        
      readStore ! BucketKeys(data.bucket)
      expectMsg(Nil)
      
      readStore ! StoreGet("not_exists")
      expectMsg(GetResp(None))
    }

    "substitute update old version with new" in {
      val vectorClock = data.vc.:+("node1")

      writeStore ! StorePut(data.copy(vc = vectorClock))
      expectMsg(Saved)

      val newVc = vectorClock.:+("node2")
      writeStore ! StorePut(data.copy(vc = newVc))
      expectMsg(Saved)

      readStore ! StoreGet(data.key)
      receiveOne(timeout.duration) match {
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
      expectMsgType[Conflict].broken.map{_.vc}.toSet should be eq(Set(vc1, vc2))

      readStore ! StoreGet(data.key)
      receiveOne(Duration(3, TimeUnit.SECONDS)) match {
        case GetResp(dataList) if dataList.get.size == 2 =>
          val vectorClocks = dataList.get.map(_.vc)
          assertResult(Set(vc1,vc2))(vectorClocks.toSet)

        case incorrect => fail(s"Concurrent vector clock not persisted, Rez = $incorrect")
      }

      val mergeVersion = vc1.merge(vc2)
      writeStore ! StorePut(data.copy(vc = mergeVersion))
      expectMsgType[PutStatus] should equal(Saved)

      readStore ! StoreGet(data.key)
      receiveOne(Duration(3, TimeUnit.SECONDS)) match {
        case GetResp(Some(dataList)) if dataList.size == 1 =>
          val vectorClocks = dataList.map(d => d.vc)
          assert(vectorClocks.size == 1)
          assert(vectorClocks.contains(mergeVersion))
        case e => fail(s"Merged version not substitute conflict, Rez = $e")

      writeStore ! StoreDelete(data.key)
      expectMsgType[String] should equal("ok")
      }
    }

    "save data if bucket contains another key" in {

      Thread.sleep(200)
      val d1 = new Data("k_1", hashing.findBucket("k_1"), 777, new VectorClock(), ByteString("value"))
      val d2 = new Data("k2", hashing.findBucket("k2"), 1231, new VectorClock(), ByteString("value2"))
      assert(hashing.findBucket(d1.key) == hashing.findBucket(d2.key))

      writeStore ! BucketPut(List(d1))
      Thread.sleep(200)
      readStore ! StoreGet(d1.key)
      expectMsgType[GetResp] should equal(GetResp(Some(List(d1))))

      readStore ! BucketKeys(d1.bucket)
      expectMsg(List(d1.key))

      writeStore ! StorePut(d2)
      expectMsg(Saved)

      readStore ! BucketKeys(d1.bucket)
      expectMsg(List(d2.key, d1.key))

      readStore ! StoreGet(d2.key)
      expectMsg(GetResp(Some(List(d2))))

      readStore ! StoreGet(d1.key)
      expectMsg(GetResp(Some(List(d1))))

      readStore ! BucketGet(d1.bucket)
      expectMsgType[GetBucketResp] should equal(GetBucketResp(d1.bucket, List(d1, d2)))

      get_delete_get(d1)
      readStore ! BucketKeys(d1.bucket)
      expectMsg(List(d2.key))
      get_delete_get(d2)  
      readStore ! BucketKeys(d1.bucket)
      expectMsg(Nil)

      readStore ! BucketGet(d2.bucket)  
      expectMsgType[GetBucketResp] should equal(GetBucketResp(d1.bucket, Nil))
    }
  }

  def get_delete_get(d: Data) {
    readStore ! StoreGet(d.key)
    expectMsg(GetResp(Some(List(d))))

    writeStore ! StoreDelete(d.key)
    expectMsg("ok")

    readStore ! StoreGet(d.key)
    expectMsg(GetResp(None))
  }

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
    FileUtils.deleteRecursively(new File("./store_spec"))
  }
}
