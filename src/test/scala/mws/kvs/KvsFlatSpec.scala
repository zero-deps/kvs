package mws.kvs

import org.scalatest._
import org.scalatest.matchers._
import org.scalatest.concurrent._
import org.scalatest.concurrent.ScalaFutures._

import akka.actor.{ActorSystem}
import akka.pattern.ask
import akka.testkit._
import akka.testkit.TestKit
import akka.testkit.TestEvent._

import com.typesafe.config.ConfigFactory

import scala.concurrent.{Await,Future}
import scala.concurrent.duration._
import scala.language.postfixOps

class KvsFlatSpec(_system:ActorSystem) extends TestKit(_system)
//  with ShouldMatchers
  with DefaultTimeout
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with GivenWhenThen
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("st",ConfigFactory.parseString("""
    kvs {
      store="mws.kvs.store.Leveldb"
    }
    leveldb {
      dir = storage
      fsync = on
      checksum = off
      native = on
    }
  """)))

  implicit val kvs:Kvs = Kvs(system)

  override def afterAll = {
    kvs.close
    TestKit.shutdownActorSystem(system)
  }

  "kvs" should "have the configured and ready" in {
    info(s"${kvs.config}")
    whenReady(kvs.isReady){r=>
      r should be (true)
    }
  }

  it should "perform DBA operation with Tuple2[String,String] type" in {
    val el1 = ("k1","v1")
    info(s"put $el1")
    val res = kvs.put(el1).fold(l=>"", r=>r)
    res should be (el1)

    val v1 = kvs.get[D]("k1").fold(l=>"", r=>r)
    info(s"get(k1) = $v1")
    v1 should be (el1)

    val v2 = kvs.get[D]("k2").fold(l=>Dbe.unapply(l).get, r=>"")
    info(s"unknown key should return $v2")
    v2 should be (("error","not_found"))

    val v3 = kvs.delete[D]("k1").fold(l=>"", r=>r)
    info(s"delete k1 key, return $v3")
    v3 should be (el1)

    val v4 = kvs.delete[D]("k1").fold(l=>Dbe.unapply(l).get, r=>"")
    info(s"value by k1 is deleted, so delete it again is $v4")
    v4 should be (("error","not_found"))
  }

  it should "perform DBA with Stats types" in {
    val sm = Message(key="k1", data="1:2:3")

    val v1 = kvs.put(sm).fold(l=>"", r=> Message.unapply(r).get)
    info(s"put $v1")

    val v2 = kvs.get[Message](s"${sm.name}.${sm.key}").fold(l=>"", r=>Message.unapply(r).get)
    info(s"get ${sm.key} = $v2")

    val v5 = kvs.delete[Message](s"${sm.name}.${sm.key}").fold(l=>"", r=>Message.unapply(r).get)
    info(s"deleted $v5")

    val smt = Metric(key="k1", data="1:2:3")
    val v3 = kvs.put(smt).fold(l=>"", r=> Metric.unapply(r).get)
    info(s"put $v3")

    val v4 = kvs.get[Metric](s"${smt.name}.${smt.key}").fold(l=>"", r=>Metric.unapply(r).get)
    info(s"get ${smt.key} = $v4")

    val v6 = kvs.delete[Metric](s"${smt.name}.${smt.key}").fold(l=>"", r=> Metric.unapply(r).get)
    info(s"deleted $v6")
  }

//  "-" should "" in {
//    val el3 = ("k2","v2","n2")
    /*implicit object dh3 extends Handler[Tuple3[String,String,String]]{
      def put(el:Tuple3[String,String,String])(implicit dba:Dba):Either[Th,Tuple3[String,String,String]] ={
        println(s"put the new object $el")
        dba.put(el._1, el._2.getBytes)
        Right(el)
      }
      def get(k:String) (implicit dba:Dba)= {
        dba.get(k)
        Right(Tuple3("simple","fucking","tuple"))
      }
      override def toString() = "DH3"
    }*/

//    println("get with specified tuple3 and implicit dh3 handler")
//    val a = Kvs.get[Tuple3[String,String,String]]("k")
//    println(s"get: $a\n")
}
