package mws.kvs

import store._

/*trait Iterable[T] { self: Handler[T] =>
  private val First = "first"
  private val Last = "last"
  private val Sep = ";"

  def add(item:Data):Unit = {
    val key = item.key
    val data = item.data

    getEntry(key) match {
      case e => println(s"entry $e")
    }
  }

  private def getEntry(key: String): Option[Entry] =
    self.get(key) map { entry =>
      val xs = entry.split(Sep, -1)
      val data = xs(0)
      val prev = if (xs(1) != "") Some(xs(1)) else None
      val next = if (xs(2) != "") Some(xs(2)) else None
      Entry(data, prev, next)
    }
*/
//  def add[T](container:String, el: T): Either[Throwable, T]
//  def remove[T](container:String, el:T): Either[Throwable, T]
//  def entries:Iterator[String]


/*  def putToList(item: Data): Unit = {
    val key = item.key
    val data = item.serialize
    getEntry(key) match {
      case Some(Entry(_, prev, next)) =>
        self.put(key, Entry(data, prev, next))
      case _ =>
        Await.result(self.get[String](Last),1 second) match {
          case Some(lastKey) =>
            // insert last
            self.put(key, Entry(data, prev = Some(lastKey), next = None))
            // link prev to last
            val last = getEntry(lastKey).get
            self.put(lastKey, last.copy(next = Some(key)))
            // update link to last
            self.put(Last, key)
          case None =>
            self.put(key, Entry(data, prev = None, next = None))
            self.put(First, key)
            self.put(Last, key)
        }
    }
  }

  def entries: Iterator[String] =
    Iterator.iterate {
      val k = Await.result(self.get[String](First,classOf[String]),1 second)
      k.flatMap(getEntry)
    } { _v =>
      val k = _v.flatMap(_.next)
      k.flatMap(getEntry)
    } takeWhile(_.isDefined) map(_.get.data)

  def deleteFromList(el: Data): Unit = {
    val key = el.key
    getEntry(key) map { case Entry(_, prev, next) =>
      prev match {
        case Some(prev) =>
          getEntry(prev) map { entry =>
            self.put(prev, entry.copy(next = next))
          }
        case _ =>
      }
      next match {
        case Some(next) =>
          getEntry(next) map { entry =>
            self.put(next, entry.copy(prev = prev))
          }
        case _ =>
      }
      (Await.result(self.get[String](First,classOf[String]),1 second),
       Await.result(self.get[String](Last,classOf[String]),1 second)) match {
        case (Some(`key`), Some(`key`)) =>
          self.delete(First)
          self.delete(Last)
        case (Some(`key`), _) =>
          self.put(First, next.get)
        case (_, Some(`key`)) =>
          self.put(Last, prev.get)
        case _ =>
      }
      self.delete(key)
  }
  }

  def first: Option[String] = Await.result(self.get[String](First,classOf[String]), 1 second).flatMap(getEntry).map(_.data)
  def last: Option[String] = Await.result(self.get[String](Last,classOf[String]),1 second).flatMap(getEntry).map(_.data)

  private def getEntry(key: String): Option[Entry] =
    Await.result(self.get[String](key,classOf[String]),1 second) map { entry =>
      val xs = entry.split(Sep, -1)
      val data = xs(0)
      val prev = if (xs(1) != "") Some(xs(1)) else None
      val next = if (xs(2) != "") Some(xs(2)) else None
      Entry(data, prev, next)
    }
  implicit private def serialize(entry: Entry): String =
    List(
      entry.data,
      entry.prev.getOrElse(""),
      entry.next.getOrElse("")
    ).mkString(Sep)
*/
//}


/**
 * KVS Handler for specific type T.
 * object Handler holds implicit default handlers.
 */
trait Handler[T] {
  def put(el:T)(implicit dba:Dba):Either[Err, T]
  def get(k:String)(implicit dba:Dba):Either[Err,T]
  def delete(k:String)(implicit dba:Dba):Either[Err,T]
  def add(el:T)(implicit dba:Dba):Either[Err,T]
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
  }

  implicit object dh extends DHandler

  trait StatMessageHandler extends Handler[Message]{
    def put(el:Message)(implicit dba:Dba) = dba.put(s"${el.name}.${el.key}", el.data.getBytes) match {
      case Right(v) => Right(el.copy())
      case Left(e) => Left(e)
    }

    def get(k:String)(implicit dba:Dba) = dba.get(k) match {
      case Right(v) => Right(Message(key=k.stripPrefix("message."), data=new String(v)))
      case Left(e) => Left(e)
    }

    def delete(k:String)(implicit dba:Dba) = dba.delete(k) match {
      case Right(v) => Right(Message(key=k.stripPrefix("message."), data=new String(v)))
      case Left(e)  => Left(e)
    }

    def add(el:Message)(implicit dba:Dba) = {
      println(s"add -> $el")

      val key = s"${el.name}.${el.key}"
      val data = el.data

      def en(k:String):Either[Err,Message] = get(k).right.map { msg =>
        println(s"map the right $msg")
        val xs = msg.data.split(Sep, -1)
        val data = xs(0)
        val prev = if (xs(1) != "") Some(xs(1)) else None
        val next = if (xs(2) != "") Some(xs(2)) else None
        msg.copy(data=data, prev=prev, next=next)
      }

      en(key) match {
        case Right(a @ Message(_,_,_,prev,next)) => {
          println("put the entry")
//          put(key, Entry(data, prev, next))
          Right(a)
        }
        case Left(l) => {
          println(s"no entry found $l")
          dba.get(Last) match {
            case Right(lastKey) =>
              println("some last returnel")
              // insert last
//              put(key, Entry(data, prev = Some(lastKey), next = None))
              // link prev to last
              //val last = en(lastKey).right
//              put(lastKey, last.copy(next = Some(key)))
              // update link to last
//              put(Last, key)
              Right(el)
            case Left(l) =>
              println(s"no last $l")
//              put(key, Entry(data, prev = None, next = None))
//              put(First, key)
//              put(Last, key)
              Left(l)
          }
        }
      }
    }
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
    def by[T,S](f:T=>S)(g:S=>T) = this
  }

  // define same handler as for message
  val f  = (a:Metric) => Message(name="message",key=a.key, data=a.data)
  val g  = (b:Message)=> Metric( name="metric", key=b.key, data=b.data)
  val fk = (k:String) => s"message${k.stripPrefix("metric")}"

  implicit val lmt = Handler.by[Metric,Message](f)(g)(fk)

}
