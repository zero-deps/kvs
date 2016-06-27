package mws.kvs
package handle

import scala.language.{higherKinds,implicitConversions}
import scalaz._, Scalaz._, Tags._
import scala.pickling._, binary._, Defaults._, static._

/**
  * Schema is the set of entry markers and specific tagged handlers.
  */
object Schema {
  sealed trait Msg
  type Message = En[String] @@ Msg
  implicit def Message(a: En[String]): En[String] @@ Msg = Tag[En[String], Msg](a)

  implicit object msgHandler extends Handler[Message]{
    val enh = implicitly[Handler[En[String]]]
    import mws.kvs.store.Dba

    def add(el: Message)(implicit dba: Dba): Either[Err,Message] = enh.add(Tag.unwrap(el)).right.map(Message)
    def remove(el: Message)(implicit dba: Dba): Either[Err,Message] =  enh.remove(Tag.unwrap(el)).right.map(Message)
    def entries(fid: String,from: Option[Message],count: Option[Int])(implicit dba: Dba): Either[Err,List[Message]] =
      enh.entries(fid,from.map(Tag.unwrap(_)),count).right.map(_.map(Message))

    def pickle(e: Message): Array[Byte] = enh.pickle(Tag.unwrap(e))
    def unpickle(a: Array[Byte]): Message = Message(enh.unpickle(a))
  }
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
  val usrFeeds = List(Left("favorite"), Left("recent"))

  final case class Game(id:String,title:String,body:String)

  sealed trait Fav
  type Favorite = En[Game] @@ Fav
  implicit def Favorite(g:En[Game]):En[Game] @@ Fav = Tag[En[Game], Fav](g)

  sealed trait Rct
  type Recent = En[Game] @@ Rct
  implicit def Recent(g:En[Game]):En[Game] @@ Rct = Tag[En[Game], Rct](g)

  implicit object gameHandler extends EnHandler[Game]{
    def pickle(e:En[Game]) = e.pickle.value
    def unpickle(a:Array[Byte]) = a.unpickle[En[Game]]
  }

  def en2f(e:En[Game]):Favorite = Favorite(e)
  def f2en(f:Favorite):En[Game] = Tag.unwrap(f)
  implicit val favHandler:Handler[Favorite] = Handler.by[Favorite,En[Game]](f2en)(en2f)(identity(_))

  def en2r(e:En[Game]):Recent = Recent(e)
  def r2en(r:Recent):En[Game] = Tag.unwrap(r)
  implicit val revHandler:Handler[Recent] = Handler.by[Recent,En[Game]](r2en)(en2r)(identity(_))
}
