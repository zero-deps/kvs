package mws.kvs
package handle

import scala.language.{higherKinds,implicitConversions}
import scalaz._, Scalaz._, Tags._
import scala.pickling._, binary._, Defaults._

/**
  * Schema is the set of entry markers and specific tagged handlers.
  */
object Schema {
  sealed trait Msg
  type Message = En[String] @@ Msg
  implicit def Message(a: En[String]): En[String] @@ Msg = Tag[En[String], Msg](a)

  private def en2m(e:En[String]):Message = Message(e)
  private def m2en(m:Message):En[String] = Tag.unwrap(m)

  implicit val msgHandler:Handler[Message] = Handler.by[Message,En[String]](m2en)(en2m)(identity(_))
}

/**
 * Social schema
 */
object SocialSchema {
  // use tags?
  type FeedName = String
  type FeedId = String
  type Feeds = Vector[Either[FeedName,Tuple2[FeedName,FeedId]]]

  trait Usr
  type User = En[Feeds] @@ Usr
  implicit def User(u:En[Feeds]):En[Feeds] @@ Usr = Tag[En[Feeds], Usr](u)

  implicit object enFeedsHandler extends EnHandler[Feeds]{
    def pickle(e:En[Feeds]) = e.pickle.value
    def unpickle(a:Array[Byte]) = a.unpickle[En[Feeds]]
  }

  def en2u(e:En[Feeds]):User = User(e)
  def u2en(s:User):En[Feeds] = Tag.unwrap(s)

  implicit val usrHandler:Handler[User] = Handler.by[User,En[Feeds]](u2en)(en2u)(identity(_))
}

/**
 * User Games schema
 */
object GamesSchema {
  val usrFeeds = Vector(Left("favorite"), Left("recent"))

  type Game = Vector[(String,String)]

  sealed trait Fav
  type Favorite = En[Game] @@ Fav
  implicit def Favorite(g:En[Game]):En[Game] @@ Fav = Tag[En[Game], Fav](g)

  sealed trait Rec
  type Recent = En[Game] @@ Rec
  implicit def Recent(g:En[Game]):En[Game] @@ Rec = Tag[En[Game], Rec](g)

  implicit object gameHandler extends EnHandler[Game]{
    def pickle(e:En[Game]) = e.pickle.value
    def unpickle(a:Array[Byte]) = a.unpickle[En[Game]]
  }

  def en2f(e:En[Game]):Favorite = Favorite(e)
  def f2en(f:Favorite):En[Game] = Tag.unwrap(f)
  implicit val favHandler:Handler[Favorite] = Handler.by[Favorite,En[Game]](f2en)(en2f)(identity(_))

  def en2r(e:En[Game]):Recent = Recent(e)
  def r2en(r:Recent):En[Game] = Tag.unwrap(r)
  implicit val recHandler:Handler[Recent] = Handler.by[Recent,En[Game]](r2en)(en2r)(identity(_))
}
