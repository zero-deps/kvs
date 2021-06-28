package zd.kvs
package search

import scala.annotation.tailrec
import proto.*

final case class Fd
  ( @N(1) id: String
  , @N(2) top: String = empty
  )

case class En
  ( @N(1) fid: String
  , @N(2) id: String
  , @N(3) prev: String = empty
  )

given MessageCodec[En] = caseCodecAuto

/**
 * Linked list of entries
 *
 * [top] -->prev--> [entry] -->prev--> [empty]
 */
object EnHandler:
  given MessageCodec[Fd] = caseCodecAuto[Fd]

  def put(fd: Fd)(using dba: Dba): Either[Err, Fd] =
    dba.put(fd.id, encode(fd)).map(_ => fd)
  
  def get(fd: Fd)(using dba: Dba): Either[Err, Option[Fd]] =
    dba.get(fd.id).map(_.map(decode))

  def delete(fd: Fd)(using dba: Dba): Either[Err, Unit] =
    dba.delete(fd.id).void

  private inline def key(fid: String, id: String): String = s"${fid}.${id}"
  private inline def key(en: En): String = key(fid=en.fid, id=en.id)

  private def _put(en: En)(using dba: Dba): Either[Err, En] =
    dba.put(key(en), encode(en)).map(_ => en)
  
  def get(fid: String, id: String)(using dba: Dba): Either[Err, Option[En]] =
    dba.get(key(fid, id)).map(_.map(decode))
  
  private def getOrFail(fid: String, id: String)(using dba: Dba): Either[Err, En] =
    val k = key(fid, id)
    dba.get(k).flatMap{
      case Some(x) => Right(decode(x))
      case None => Left(KeyNotFound)
    }

  private def delete(fid: String, id: String)(using dba: Dba): Either[Err, Unit] =
    dba.delete(key(fid, id)).void

  /**
   * Adds the entry to the container
   * Creates the container if it's absent
   * @param en entry to add (prev is ignored). If id is empty it will be generated
   */
  def add(en: En)(using dba: Dba): Either[Err, En] =
    get(Fd(en.fid)).flatMap(_.cata(Right(_), put(Fd(en.fid)))).flatMap{ (fd: Fd) =>
      ( if (en.id == empty)
          dba.nextid(en.fid) // generate ID if it is empty
        else
          get(en.fid, en.id).flatMap( // id of entry must be unique
            _.cata(_ => Left(EntryExists(key(en))), Right(en.id))
          )
      ).map(id => en.copy(id=id, prev=fd.top)).flatMap{ en =>
        // add new entry with prev pointer
        _put(en).flatMap{ en =>
          // update feed's top
          put(fd.copy(top=en.id)).map(_ => en)
        }
      }
    }

  /**
   * Iterates through container and return the stream of entries.
   *
   * Stream is FILO ordered (most recent is first).
   * @param from if specified then return entries after this entry
   */
  def all(fid: String)(using Dba): Either[Err, LazyList[Either[Err, En]]] =
    all(fid, from=None)

  def all(fid: String, from: Option[En])(using Dba): Either[Err, LazyList[Either[Err, En]]] =
    def _stream(id: String): LazyList[Either[Err, En]] =
      id match
        case `empty` => LazyList.empty
        case _ =>
          val en = getOrFail(fid, id)
          en match
            case Right(e) => LazyList.cons(en, _stream(e.prev))
            case _ => LazyList(en)
    from match
      case None => get(Fd(fid)).map(_.cata(x => _stream(x.top), LazyList.empty))
      case Some(en) => Right(_stream(en.prev))

  def remove(_fid: String, _id: String)(using Dba): Either[Err, Unit] =
    // get entry to delete
    getOrFail(_fid, _id).flatMap{ en =>
      val id = en.id
      val fid = en.fid
      val prev = en.prev
      get(Fd(fid)).flatMap(_.cata(Right(_), Left(KeyNotFound))).flatMap{ fd =>
        val top = fd.top
        ( if (id == top)
            // change top and decrement
            put(fd.copy(top=prev))
          else
            // find entry which points to this one (next)
            LazyList.iterate(start=getOrFail(fid,top))(_.flatMap(x=>getOrFail(fid,x.prev)))
              .takeWhile(_.isRight)
              .flatMap(_.toOption)
              .find(_.prev==id)
              .toRight(KeyNotFound)
              // change link
              .flatMap(next => _put(next.copy(prev=prev)))
        ).flatMap(_ => delete(fid, id)) // delete entry
      }
    }
