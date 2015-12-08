package mws.kvs
package handle

import store._

import scala.language.postfixOps
import scalaz._
import Scalaz._

/**
 * Entry type handler.
 */
trait EHandler extends Handler[En]{
  import Handler._
  private val Sep = ";"

  val feedHandler = implicitly[Handler[Fd]]

  private[this] def ns(el:En):String = el._1
  private[this] def key(el:En):Id = s"${el._1}.${el._2}"

  private[this] def toNs(s:String):Either[Err,(Fid,Id)] = s.split('.') match {
    case Array(fid,eid) => Right((fid,eid))
    case k => println(s"${s} broken"); Left(Dbe(msg="broken_key")) }

  private[this] def pickle(el:En):Array[Byte] =
    (el._5 + Sep + el._3.getOrElse("none") + Sep +el._4.getOrElse("none")).getBytes

  private[this] def depickle(d:Array[Byte]):Tuple3[String,Option[String],Option[String]] = {
    val xs:Array[String] = new String(d).split(Sep, -1)
    val data = xs(0)
    val prev = if (xs(1) != "none") Some(xs(1)) else None
    val next = if (xs(2) != "none") Some(xs(2)) else None
    (data,prev,next)
  }

  def add(el: En)(implicit dba: Dba): Either[Err,En] = {
    val k = key(el)
    val ckey = ns(el)

    val feed = feedHandler.get(ckey).left.map {
      case Dbe("error","not_found") => feedHandler.put(new Fd(ckey,None,0))}.joinLeft

    feed.right.map { c:Fd => get(k) match {
      case Right(a: En) => Left(Dbe(msg="exist"))
      case Left(l) => val x:Either[Err,En] = c._2 match {
        case Some(top) => get(top) match {
          case Right(e) => {
            put(e.copy(_4=Some(k)));
            put(el.copy(_3=Some(top)))
          }
          case Left(l) => Left(Dbe(msg=s"broken_links"))
        }
        case None => put(el.copy(_1=c._1))
      }
      x.right.map { aa:En => feedHandler.put(c.copy(_2=Some(k),_3=c._3 + 1)); aa }
    }}.joinRight
  }

  val skip:Either[Err,Fd] = Left(Dbe(msg="skip"))

  def remove(el: En)(implicit dba: Dba): Either[Err,En] = {
    val k = key(el)
    val kb = k.getBytes

    get(k).right.map { case e @ Tuple5(_,_,prev,next,_) =>
      prev map { get(_).right.map { entry => put(entry.copy(_4=next)) } }
      next map { get(_).right.map { entry=> put(entry.copy(_3=prev)) } }

      feedHandler.get(ns(el)).right.map { c:Fd => 
        c._2 match {
          case Some(`k`) => feedHandler.put(c.copy(_2=prev,_3=c._3 - 1))
          case _ =>
        }
        c.copy()
      }
      delete(k)
    }.joinRight
  }

  val none:Either[Err,En]=Left(Dbe(msg="none"))
  def nextEntry(implicit dba:Dba): (Either[Err,En]) => Either[Err,En] = _.right.map {_._3.fold(none)(get)}.joinRight

  def entries(fid:String, from:Option[En], count:Option[Int])(implicit dba:Dba):Either[Err,List[En]] = 
    feedHandler.get(fid).right.map { case (fid,top,size) =>
    val start = (from orElse top).map{_.toString}.get
    val n = count.getOrElse(size)
    List.iterate(get(start), n)(nextEntry).sequenceU
  }.joinRight

  def get(k: String)(implicit dba: Dba): Either[Err,En] = dba.get(k) match {
    case Right(v) => toNs(k).right.map { case (fid,key) =>
      val e1 = depickle(v)
      (fid, key, e1._2, e1._3, e1._1)
    }
    case Left(e) => Left(e)
  }

  def put(el: En)(implicit dba: Dba): Either[Err,En] = dba.put(key(el), pickle(el)) match {
    case Right(v) => Right(el.copy())
    case Left(e) => Left(e)
  }

  def delete(k: String)(implicit dba: Dba): Either[Err,En] = dba.delete(k) match {
    case Right(v) => toNs(k).right.map{ case (fid,key) =>
      val d1 = depickle(v)
      (fid, key, d1._2, d1._3, d1._1)
    }
    case Left(e)  => Left(e)
  }
}
