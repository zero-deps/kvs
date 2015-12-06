package mws.kvs

import store._

/**
 * KVS Handler for specific type T.
 * object Handler holds implicit default handlers.
 */
trait Handler[T] {
  def put(el:T)(implicit dba:Dba):Either[Err, T]
  def get(k:String)(implicit dba:Dba):Either[Err,T]
  def delete(k:String)(implicit dba:Dba):Either[Err,T]
  def add(el:T)(implicit dba:Dba):Either[Err,T]
  def remove(el:T)(implicit dba:Dba):Either[Err,T]
  def entries()(implicit dba:Dba):Either[Err,Iterator[T]]
}
object Handler{
  def apply[T](implicit h:Handler[T]) = h

  private val First = "first"
  private val Last = "last"
  private val Sep = ";"

  trait DHandler extends Handler[D]{
    def put(el:D)(implicit dba:Dba) = dba.put(el._1, el._2.getBytes) match {
      case Right(v) => Right((el._1, new String(v)))
      case Left(e) => Left(e)
    }

    def get(k:String)(implicit dba:Dba) = dba.get(k) match {
      case Right(v) => Right((k,new String(v)))
      case Left(e) => Left(e)
    }

    def delete(k:String)(implicit dba:Dba) = dba.delete(k) match {
      case Right(v) => Right((k,new String(v)))
      case Left(e)  => Left(e)
    }

    def add(el:D)(implicit dba:Dba) = ???
    def remove(el:D)(implicit dba:Dba) = ???
    def entries()(implicit dba:Dba) = ???
  }

  implicit object dh extends DHandler

  trait StatMessageHandler extends Handler[Message]{
    private[this] def key(el:Message):String = s"${el.name}.${el.key}"
    private[this] def data(el:Message):Array[Byte] =
      (el.data+Sep+el.prev.getOrElse("none") +Sep+el.next.getOrElse("none")).getBytes

    private[this] def ent(d:Array[Byte]):Tuple3[String,Option[String],Option[String]] = {
      val xs:Array[String] = new String(d).split(Sep, -1)
      val data = xs(0)
      val prev = if (xs(1) != "none") Some(xs(1)) else None
      val next = if (xs(2) != "none") Some(xs(2)) else None

      (data,prev,next)
    }

    def put(el:Message)(implicit dba:Dba) = dba.put(key(el), data(el)) match {
      case Right(v) => Right(el.copy())
      case Left(e) => Left(e)
    }

    def get(k:String)(implicit dba:Dba) = dba.get(k) match {
      case Right(v) => val e1 = ent(v)
        Right(Message(key=k.stripPrefix("message."), data= e1._1,prev= e1._2, next= e1._3))
      case Left(e) => Left(e)
    }

    def delete(k:String)(implicit dba:Dba) = dba.delete(k) match {
      case Right(v) => Right(Message(key=k.stripPrefix("message."), data=new String(v)))
      case Left(e)  => Left(e)
    }

    def add(el:Message)(implicit dba:Dba) = {
      val k = key(el)
      get(k) match {
        case Right(a: Message) => Left(Dbe(msg="exist"))
        case Left(l) => dba.get(Last) match {
          case Right(lkey) =>
            val lastK = new String(lkey)

            get(lastK) match {
              case Right(e) => {
                dba.put(Last, k.getBytes)
                put(e.copy(next=Some(k))).right.map {_=>
                  put(el.copy(prev=Some(lastK)))
                }.joinRight
              }
              case Left(l) => Left(Dbe(msg=s"broken_links"))
            }

          case Left(l) =>
            dba.put(Last, k.getBytes)
            dba.put(First, k.getBytes)
            put(el)
        }
      }
    }

    def remove(el:Message)(implicit dba:Dba) = {
      val k = key(el)
      val kb = k.getBytes

      get(k).right.map { case e @ Message(_,_,_,prev,next) =>
        prev match {
          case Some(p) => get(p).right.map { entry => put(entry.copy(next=next)) }
          case _=>
        }// ! errors
        next match{
          case Some(n)=> get(n).right.map { entry=> put(entry.copy(prev=prev)) }
          case _ =>
        }///! errors

        (dba.get(First), dba.get(Last)) match {
          case (Right(a),Right(b)) if (new String(a).equals(k) && new String(b).equals(k))=>
            dba.delete(First)
            dba.delete(Last)
          case (Right(r),_) if new String(r).equals(k) => dba.put(First,next.get.getBytes)
          case (_,Right(r)) if new String(r).equals(k) => dba.put(Last, prev.get.getBytes)
          case _ =>
        }
        delete(k)
      }.joinRight
    }

    def entries()(implicit dba:Dba):Either[Err, Iterator[Message]] =
      dba.get(First).right.map { bs =>
        get(new String(bs)).right.map { msg =>

          Iterator.iterate(Option(msg)) { m =>
            m.get.next.map { x =>
              get(x) match {
                case Right(xx) => xx
                case Left(l) => m.get.copy(next=None)
              }
            }
          } takeWhile(_.isDefined) map(_.get)
        }
      }.joinRight
  }

  implicit object lm extends StatMessageHandler

  /**
   * Given handler S create the handler for T from conversion functions.
   */
  def by[T,S](f:T=>S)(g:S=>T)(ky:String=>String)(implicit h:Handler[S]):Handler[T] = new Handler[T] {
    def put(el:T)(implicit dba:Dba):Either[Err,T] = h.put(f(el)).right.map(g)
    def get(k:String)(implicit dba:Dba):Either[Err,T] = h.get(ky(k)).right.map(g)
    def delete(k:String)(implicit dba:Dba):Either[Err,T] = h.delete(ky(k)).right.map(g)
    def add(el:T)(implicit dba:Dba):Either[Err,T] = h.add(f(el)).right.map(g)
    def remove(el:T)(implicit dba:Dba):Either[Err,T] = h.remove(f(el)).right.map(g)
    def entries()(implicit dba:Dba):Either[Err,Iterator[T]] = h.entries().right.map { _ map g }
    def by[T,S](f:T=>S)(g:S=>T) = this
  }

  // define same handler as for message
  val f  = (a:Metric) => Message(name="message",key=a.key, data=a.data)
  val g  = (b:Message)=> Metric( name="metric", key=b.key, data=b.data)
  val fk = (k:String) => s"message${k.stripPrefix("metric")}"

  implicit val lmt = Handler.by[Metric,Message](f)(g)(fk)

}
