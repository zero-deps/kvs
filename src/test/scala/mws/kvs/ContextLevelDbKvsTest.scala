package mws.core.service.kvs

import java.io.File

import akka.serialization.SerializationExtension
//import com.playtech.mws.api.Context
import com.typesafe.config.ConfigFactory
//import mws.core.ActorTest
import org.fusesource.leveldbjni.JniDBFactory
import org.iq80.leveldb.Options


object ContextLevelDbKvsTest {
  val config = ConfigFactory.parseResources("testapplication.conf")
}

//@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ContextLevelDbKvsTest {//extends ActorTest(ContextLevelDbKvsTest.config) {

/*  val dbFile = new File("testContextLevelDbKvs")
  val options = new Options()
  options.createIfMissing(true)
  val db = JniDBFactory.factory.open(dbFile, options)
  val contextLevelDbKvs = new ContextLevelDbKvs(db, "contextKvs", SerializationExtension(system))

  test("contextLevelDbTest") {
    val ctx = Context(user = null, umsSecretKey = "secret", remoteAddress = Some("localhost"),
      properties = null, correlationId = Some("corId"), sessionId = 1)
    val key1 = "ctx1"
    val putResult = contextLevelDbKvs.put(key1, ctx)
    assert(putResult.isRight, "Should be right")
    assert(putResult.right.get == ctx, "Context should be equal")

    val getResult = contextLevelDbKvs.get(key1)
    assert(getResult.isRight, "Should be right")
    val op = getResult.right.get
    assert(op.isDefined, "Option should be defined")
    assert(op.get == ctx, "Context should be equal")

    val getResult2 = contextLevelDbKvs.get(key1 + "bla bla")
    assert(getResult2.isRight, "Should be right")
    val op2 = getResult2.right.get
    assert(op2.isEmpty, "Option should be empty")

    val deleteResult = contextLevelDbKvs.remove(key1)
    assert(deleteResult.isRight, "Should be right")
    val deleteOp = deleteResult.right.get
    assert(deleteOp.isDefined, "Option should be defined")
    assert(deleteOp.get == ctx, "Context should be equal")

    val getResult3 = contextLevelDbKvs.get(key1)
    assert(getResult3.isRight, "Should be right")
    val op3 = getResult3.right.get
    assert(op3.isEmpty, "Option should be empty")
  }

  override protected def afterAll(): Unit = {
    try {
      db.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }

    try {
      dbFile.listFiles() foreach(_.delete())
      dbFile.delete()
    } catch {
      case e: Exception => e.printStackTrace()
    }

    super.afterAll()
  }*/
}
