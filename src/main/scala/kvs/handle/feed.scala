package mws.kvs
package handle

import store._

/**
 * Feed type handler.
 */
trait FdHandler extends Handler[Fd]{
  private[this] def key(el:Fd):Id = el._1
  private[this] def pickle(el:Fd):Array[Byte] = s"""${el._2.getOrElse("none")},${el._3}""".getBytes
  private[this] def depickle(d:Array[Byte]):Tuple2[Option[String],Int] = {
    new String(d).split(",") match {
      case Array(top,count) => (if (top!="none") Some(top) else None, count.toInt)
      case _=> println("failed to parse fd");(None, 0)
    }
  }

  def add(el: Fd)(implicit dba: Dba): Either[Err,Fd] = ???
  def delete(k: String)(implicit dba: Dba): Either[Err,Fd] = dba.delete(k) match {
    case Right(v) => {
      val d1 = depickle(v)
      Right((k, d1._1, d1._2))
    }
    case Left(e)  => Left(e)
  }

  def entries(fid:String, from:Option[Fd], count:Option[Int])(implicit dba: Dba): Either[Err,List[Fd]] = {
    println(s"entries $fid")
    Right(Nil)
  }

  def get(k: String)(implicit dba: Dba): Either[Err,Fd] = dba.get(k) match {
    case Right(v) => {
      val e1 = depickle(v)
      Right((k, e1._1, e1._2))
    }
    case Left(e) => Left(e)
  }

  def put(el: Fd)(implicit dba: Dba): Either[Err,Fd] = dba.put(key(el), pickle(el)) match {
    case Right(v) => Right(el.copy())
    case Left(e) => Left(e)
  }
  def remove(el: Fd)(implicit dba: Dba): Either[Err,Fd] = ???
}
