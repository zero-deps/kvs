package mws.kvs

import org.scalatest._
import org.scalatest.matchers._
import org.scalatest.concurrent._
import org.scalatest.concurrent.ScalaFutures._

import org.scalactic._

import akka.actor.ActorSystem
import akka.testkit._
import akka.testkit.TestKit
import akka.testkit.TestEvent._

import mws.kvs._
import mws.kvs.store._
import com.typesafe.config._

import scalaz._
//import Scalaz._

import store._
import handle._

class KvsSpec(_system:ActorSystem) extends TestKit(_system)
  with DefaultTimeout
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("uHt",ConfigFactory.load))

  implicit val kvs:Kvs = Kvs(system)

  override def afterAll = {
    kvs.close
    TestKit.shutdownActorSystem(system)
  }

  import Handler._
  import UserHandler._

  "kvs" should "have the configured and ready" in {
    whenReady(kvs.isReady){r => r should be (true)}
  }

  "user handler" should "implement handler API for user" ignore {
    val u1 = User("users","k1", None, None, "{email:andrii.zadorozhnii@gmail.com}")
    val u2 = u1.copy(key="k2")
    val u3 = u1.copy(key="k3")

//    val f = (u:User) => (u.cn, u.key, u.prev, u.next, u.data)
//    val g = (e:En) => User(cn=e._1,key=e._2, prev=e._3, next=e._4, data=e._5)
//    val k = (s:String) => s

//    implicit val uh = Handler.by[User,En](f)(g)(k)

//    u1 should be(g(f(u1)))

    // ns,id
    val ff = (e:Feed) => (e.id, None, 0)
    val gg = (e:Fd) => Feed(ns="system",id=e._1)
    val kk = (s:String) => s

    implicit val fh = Handler.by[Feed,Fd](ff)(gg)(kk)

    println("\t---container check----")
    val f1 = kvs.get[Feed]("users")
    println(s"\tusers feed: $f1")

    val r = kvs.add(u1)
    println(s"""add:${r.fold(l=>l, r=> User.unapply(r).get)}""")

    println("\t---container check----")
    val f2 = kvs.get[Feed]("users")
    println(s"\tusers feed: $f2")

    val r2 = kvs.add(u2)
    println(s"""add:${r2.fold(l=>l, r=> User.unapply(r).get)}""")

    val r3 = kvs.add(u3)
    println(s"""add:${r3.fold(l=>l, r=> User.unapply(r).get)}""")

    println("\t---container check----")
    val f3 = kvs.get[Feed]("users")
    println(s"\tusers feed: $f3")

    val e = kvs.entries[User]("users",None,None)
    println(e)
//    val i:List[User] = e.fold(l=>Nil, r=>r)
//    println(s"${i}")

//    val xl = List(1, 2, 3) >| "x"
//    println(xl)
//    val tl = (1,2,3) map {_ + 1}
//    println(tl)
//    (3*2)*(8*5) assert_=== 3*(2*(8*5))
//    true should be(true)

    kvs.remove(u1)
    kvs.remove(u2)
    kvs.remove(u3)
    kvs.delete[Feed]("users")
  }


}
