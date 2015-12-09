package mws.kvs
package handle

import store._

/**
 * Handle User as feed entry.
 */
object UserHandler {
/*  val f = (u:User) => (Feed.unapply(u.cn).get, u.key, u.prev, u.next, u.data)
  val g = (e:En) => User(cn=Feed.tupled(e._1),key=e._2, prev=e._3, next=e._4, data=e._5)

  implicit val uh = Handler.by[User,En](f)(g)((s:String) => s)
*/
  val f = (u:User) => (u.cn, u.key, u.prev, u.next, u.data)
  val g = (e:En[String]) => User(cn=e._1,key=e._2, prev=e._3, next=e._4, data=e._5)
  val k = (s:String) => s

  implicit val uh = Handler.by[User,En[String]](f)(g)(k)
}
