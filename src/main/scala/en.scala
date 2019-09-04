package zd.kvs
package en

import zd.kvs.store.Dba
import zd.gs.z._
import zd.proto.api.{N, MessageCodec, encode, decode}
import zd.proto.macrosapi.{caseCodecAuto}
import scala.collection.immutable.ArraySeq

final case class AddAuto(fid: String, data: ArraySeq[Byte])
final case class Add(fid: String, id: String, data: ArraySeq[Byte])
final case class En
  ( @N(1) fid: String
  , @N(2) id: String
  , @N(3) prev: Option[String]
  , @N(4) data: ArraySeq[Byte]
  )

/**
 * Abstract type entry handler
 * Since we don't know the exact type the pickler/unpickler still needs to be provided explicitly
 *
 * [top] -->prev--> [en] -->prev--> [none]
 */
object EnHandler {
  private val fh = FdHandler
  private implicit val c1: MessageCodec[Byte] = ??? //todo: delete
  private implicit val codec: MessageCodec[En] = caseCodecAuto[En]
  private def pickle(e: En): Res[Array[Byte]] = encode[En](e).right
  private def unpickle(a: Array[Byte]): Res[En] = decode[En](a).right

  private def key(fid: String, id: String): String = s"${fid}.${id}"
  private def _put(en: En)(implicit dba:Dba):Res[En] = {
    for {
      p <- pickle(en)
      _ <- dba.put(key(en.fid, en.id), p)
    } yield en
  }
  def get(fid: String, id: String)(implicit dba: Dba): Res[Option[En]] = {
    dba.get(key(fid,id)) match {
      case Right(Some(x)) => unpickle(x).map(_.just)
      case Right(None) => None.right
      case x@Left(_) => x.coerceRight
    }
  }
  private def _get(fid: String, id: Option[String])(implicit dba: Dba): Res[En] = {
    id match {
      case Some(id) => _get(fid, id)
      case None => Fail("id is empty").left
    }
  }
  private def _get(fid: String, id: String)(implicit dba: Dba): Res[En] = {
    val k = key(fid, id)
    dba.get(k) match {
      case Right(Some(x)) => unpickle(x)
      case Right(None) => Fail(k).left
      case x@Left(_) => x.coerceRight
    }
  }
  private def delete(fid:String,id:String)(implicit dba:Dba):Res[Unit] = dba.delete(key(fid,id))

  private def to_en(en: AddAuto, id: String, prev: Option[String]): En = ???
  private def to_en(en: Add, prev: Option[String]): En = ???

  /**
   * Adds the entry to the container. Creates the container if it's absent.
   * @param en entry to add. ID will be generated.
   */
  def add(addEn: AddAuto)(implicit dba: Dba): Res[En] = {
    val fid = addEn.fid
    for {
      fd1 <- fh.get(Fd(fid))
      fd <- fd1.cata(_.right, fh.put(Fd(fid)).map(_ => Fd(fid)))
      id <- dba.nextid(fid)
      en = to_en(addEn, id, fd.top)
      _ <- _put(en)
      _ <- fh.put(fd.copy(top=en.id.just, count=fd.count+1))
    } yield en
  }

  /**
   * Adds the entry to the container. Creates the container if it's absent.
   * @param en entry to add.
   */
  def add(addEn: Add)(implicit dba: Dba): Res[En] = {
    val fid = addEn.fid
    val id = addEn.id
    for {
      fd1 <- fh.get(Fd(fid))
      fd <- fd1.cata(_.right, fh.put(Fd(fid)).map(_ => Fd(fid)))
      en1 <- get(fid, id)
      _ <- en1.cata(_ => EntryExists(key(fid, id)).left, ().right)
      en = to_en(addEn, fd.top)
      _ <- _put(en)
      _ <- fh.put(fd.copy(top=id.just, count=fd.count+1))
    } yield en
  }

  /**
   * Puts the entry to the container
   * If entry don't exists in containter create container and add it to the head
   * If entry exists in container, put it in the same place
   * @param en entry to put (prev is ignored)
   */
  def put(en: Add)(implicit dba: Dba): Res[En] = {
    for {
      x <- get(en.fid, en.id)
      z <- x.cata(y => _put(to_en(en, y.prev)), add(en))
    } yield z
  }

  /**
   * Remove the entry from the container specified.
   * @return deleted entry (with data). Or None if element is absent.
   */
  def remove(_fid: String, _id: String)(implicit dba: Dba): Res[Option[En]] = {
    for {
      en1 <- get(_fid, _id)
      _ <- en1.cata(en => {
        val fid = en.fid
        val id = _id
        val prev = en.prev
        for {
          fd1 <- fh.get(Fd(fid))
          fd <- fd1.cata(_.right, Fail(fid).left)
          top = fd.top
          _ <- if (Option(id) == top) {
            fh.put(fd.copy(top=prev, count=fd.count-1))
          } else {
            for {
              next <- LazyList.iterate(start=_get(fid,top))(_.flatMap(x=>_get(fid,x.prev))).
                takeWhile(_.isRight).
                flatMap(_.toOption).
                find(_.prev == Option(id)).
                toRight(Fail(key(fid, id)))
              _ <- _put(next.copy(prev=prev))
              _ <- fh.put(fd.copy(count=fd.count-1))
            } yield ()
          }
          _ <- delete(fid, id)
        } yield ()
      }, ().right)
    } yield en1
  }

  /** Iterates through container and return the stream of entries.
   *
   * Stream is FILO ordered (most recent is first).
   * @param from if specified then return entries after this entry
   */
  def all(fid: String, from: Option[En])(implicit dba: Dba): Res[LazyList[Res[En]]] = {
    def _stream(id: Option[String]): LazyList[Res[En]] = {
      id match {
        case None => LazyList.empty
        case Some(id) =>
          val en = _get(fid, id)
          en match {
            case Right(e) => LazyList.cons(en, _stream(e.prev))
            case _ => LazyList(en)
          }
      }
    }
    from match {
      case None => fh.get(Fd(fid)).map(_.cata(x => _stream(x.top), LazyList.empty))
      case Some(en) => _stream(en.prev).right
    }
  }
}
