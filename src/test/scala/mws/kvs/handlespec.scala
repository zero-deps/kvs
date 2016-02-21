package mws.kvs

import akka.actor.ActorSystem
import com.typesafe.config._

import org.scalatest._, matchers._, concurrent._, ScalaFutures._
import akka.testkit._, TestEvent._

import store._, handle._
import scala.language.{higherKinds,postfixOps,implicitConversions}

/**
 * Acceptance test cases for various type of handlers which requires full kvs setup with actor system, backend etc.
 */
class HandleSpec(_system:ActorSystem) extends TestKit(_system)
  with DefaultTimeout
  with ImplicitSender
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  import scala.pickling._, Defaults._, binary._, static._

  def this() = this(ActorSystem("handle", ConfigFactory.load))

  implicit val kvs:Kvs = Kvs(system)
  implicit val dba:Dba = kvs.dba

  override def afterAll = {kvs.close;TestKit.shutdownActorSystem(system)}

  "kvs" should "have the configured and ready" in {
    whenReady(kvs.isReady){r => r should be (true)}
  }

  import scalaz._,Scalaz._

  ignore should "-" in {
    val f1 = Fd("f1")
    val e1 = En("f1","e1",None,None,"d1")

    import scalaz._,Scalaz._,Tags._

//    println(s"After put: ${r1}")

//    println(s"${r1.right.map{Tag.unwrap(_)}}")

    //now we can define the method which takes tagged and retuen orig
//    def putentry[En[String]](a:En[String] @@ Message): En[String]  = Message.unwrap(a)

//    val b:En[String] = putentry(m)
//    println(s"And unwrap: $b")

//    f1 match {
//      case Message(ms) => println("this is really fucking message")
//    }

    //val me:Msg
    //val me = Msg("f1","e1",None,None,"d1")
//    def x(e:En[String]):Msg = {
//      msg(e)
//    }

//    println(s"really me? :$me  ${Tag.unwrap(me)}  ${Tag.unsubst[En[String],Id,Msg](me)}")

//    val r = kvs.put(Tag.unwrap(me))
//    val r1= kvs.put(e1)
//    println(s"$r1")

//    val e1 = Msg("f1","e1",None,None,"d1")
//    val e2 = Msg("f1","e2",None,None,"d2")
//    val e3 = Msg("f1","e3",None,None,"d3")

    //println(s"put f1: ${kvs.put(f1)}")
    //println(s"""get f1: ${kvs.get[Fd]("f1")}""")

    //println(s"put e1: ${kvs.put(e1)}")
    //println(s"""get e1: ${kvs.get[En[String]]("e1")}""")

/*
    println(s"""add e1: ${kvs.add(e1)}""")
    println(s"""add e1: ${kvs.add(e2)}""")
    println(s"""add e1: ${kvs.add(e3)}""")

    println(s"""entries: ${kvs.entries[En[String]](e1.fid)}""")

    println("\n---assert---")
    println(s"""check feed: ${kvs.get[Fd]("f1").fold(l=>l,r=>Fd.unapply(r).get)}""")

    println("---direct get check---")
    println(s"""get f1.e1: ${kvs.get[En[String]]("f1.e1")}""")
    println(s"""get f1.e2: ${kvs.get[En[String]]("f1.e2")}""")
    println(s"""get f1.e3: ${kvs.get[En[String]]("f1.e3")}""")

    println("---cleanup---")
    println(s"""rem e1: ${kvs.remove(e1)}""")
    println(s"""rem e1: ${kvs.remove(e2)}""")
    println(s"""rem e1: ${kvs.remove(e3)}""")
    println(s"""entries: ${kvs.entries[En[String]](e1.fid)}""")
    println(s"""del f1: ${kvs.delete[Fd]("f1")}""")
*/
  }

/*  "user handler" should "implement handler API for user" ignore {
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
*/

/*  ignore should "perform DBA operation with Tuple2[String,String] type" in {
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

    // add cases for existing key
  }

  ignore should "perform DBA with Stats types" in {
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

  ignore should "make the items added with `add` available throught `get` | compared ignore links" in {
    sms.map(kvs.add(_))

    val res = sms.map {sm => kvs.get[Message](s"${sm.name}.${sm.key}")}.
      map {r=> r.fold(l=>"", r=> r.copy(next=None,prev=None))}.
      map {s => info(s"$s - done");s}

    res should equal(sms)

    sms.map(kvs.remove(_))
  }

  ignore should "support entries iterator with the element in order of `add`" in {
    sms.map(kvs.add(_))

    val s:Option[Message] = None
    val e = kvs.entries[Message]("messages",s,Some(2))
//    val i:Iterator[Message] = e.fold(l=>Iterator.empty, r=>r)
//    i.toList.map{e=> info(s"$e");e}.map{_.copy(next=None,prev=None)} should equal(sms)
    println(e)

    sms.map(kvs.remove(_))
  }
*/

  it should "wrap user operations" in {
    import SocialSchema._

    val feeds:Feeds = List(Left("favs"),Left("posts"))
    val u1 = En("users","u1",None,None,feeds)

    val r1 = kvs.add(u1)
    println(s"add user: $r1")

    kvs.remove[En[Feeds]](u1)
  }
}
