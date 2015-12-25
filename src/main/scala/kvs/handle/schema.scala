package mws.kvs
package handle

import scala.language.{higherKinds,implicitConversions}
import scalaz._, Scalaz._, Tags._

import store._, handle._

/**
 * Schema is the set of entry markers and specific tagged handlers.
 */
object Schema {

  trait Msg
  trait Mtr
  trait Ses

  type Message = En[String] @@ Msg
  type Metrics = En[String] @@ Mtr
  type Session = En[String] @@ Ses

  implicit def Message(a: En[String]): En[String] @@ Msg  = Tag[En[String], Msg](a)
  implicit def Metrics(a: En[String]): En[String] @@ Mtr  = Tag[En[String], Mtr](a)
  implicit def Session(a: En[String]): En[String] @@ Ses  = Tag[En[String], Ses](a)

  /**
   * Handler for stats application message.
   */
  implicit object msgHandler extends Handler[Message]{
    val enh = implicitly[Handler[En[String]]]

    def get(k: String)(implicit dba: Dba): Either[Err,Message] = enh.get(k).right.map(Message(_))
    def put(el: Message)(implicit dba: Dba): Either[Err,Message] = enh.put(Tag.unwrap(el)).right.map(Message(_))
    def delete(k: String)(implicit dba: Dba): Either[Err,Message] = enh.delete(k).right.map(Message(_))

    def add(el: Message)(implicit dba: Dba): Either[Err,Message] = enh.add(Tag.unwrap(el)).right.map(Message(_))
    def remove(el: Message)(implicit dba: Dba): Either[Err,Message] =  enh.remove(Tag.unwrap(el)).right.map(Message(_))
    def entries(fid: String,from: Option[Message],count: Option[Int])(implicit dba: Dba): Either[Err,List[Message]] =
      enh.entries(fid,from.map(Tag.unwrap(_)),count).right.map(_.map(Message(_)))

    def pickle(e: Message): Array[Byte] = enh.pickle(Tag.unwrap(e))
    def unpickle(a: Array[Byte]): Message = Message(enh.unpickle(a))
  }

  /**
   * handler for stats application metrics. Created from the message handler.
   */
  def m2m(m:Metrics):Message = Message(Tag.unwrap(m))
  def m4m(m:Message):Metrics = Metrics(Tag.unwrap(m))

  implicit val x = Handler.by[Metrics,Message](m2m)(m4m)(identity(_))

}

/**
 * Social schema
 * 
 * todo: mark Id's with tags string/long
 */
object SocialSchema {
  type Fid = Either[String,Tuple2[String,String]] // name or (name,id)
  type Feeds = List[Fid]

  trait Usr
  type User = En[Feeds] @@ Usr
  implicit def User(u:En[Feeds]):En[Feeds] @@ Usr = Tag[En[Feeds], Usr](u)

  import scala.pickling._, binary._, Defaults._

  implicit object enFeedsHandler extends EnHandler[Feeds]{
    def pickle(e:En[Feeds]) = e.pickle.value
    def unpickle(a:Array[Byte]) = a.unpickle[En[Feeds]]
  }

  def en2u(e:En[Feeds]):User = User(e) 
  def u2en(s:User):En[Feeds] = Tag.unwrap(s)

  implicit val usrHandler:Handler[User] = Handler.by[User,En[Feeds]](u2en)(en2u)(identity(_))
}
