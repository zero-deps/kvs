package zd.kvs
package idx

import proto.*

/**
 * Feed of entries: [top] ----prev-> [en] ----prev-> [empty]
 */
object IdxHandler:
  opaque type Fid = String
  def Fid(fid: String): Fid = fid

  case class Fd
    ( @N(1) id: Fid
    , @N(2) top: String = empty
    )

  case class Idx
    ( @N(1) fid: Fid
    , @N(2) id: String
    , @N(3) prev: String
    )

  object Idx:
    def apply(fid: Fid, id: String): Idx = Idx(fid, id, empty)
    
  given MessageCodec[Fd] = caseCodecAuto

  def update(fd: Fd, top: String)(using dba: Dba): Either[Err, Unit] =
    dba.put(fd.id, encode(fd.copy(top=top))).void

  def create(fid: Fid)(using dba: Dba): Either[Err, Fd] =
    val fd = Fd(fid)
    dba.put(fd.id, encode(fd)).map(_ => fd)
  
  def get(fid: Fid)(using dba: Dba): Either[Err, Option[Fd]] =
    dba.get(fid).map(_.map(decode))
  
  def delete(fid: Fid)(using dba: Dba): Either[Err, Unit] =
    dba.delete(fid).void
  
  type A = Idx
  given MessageCodec[A] = caseCodecAuto

  private def key(fid: Fid, id: String): String = s"${fid}.${id}"

  private def _put(en: A)(using dba: Dba): Either[Err, A] =
    dba.put(key(en.fid, en.id), encode(en)).map(_ => en)
 
  def get(fid: Fid, id: String)(using dba: Dba): Either[Err, Option[A]] =
    dba.get(key(fid, id)).map(_.map(decode))

  private def getOrFail(fid: Fid, id: String)(using dba: Dba): Either[Err, A] =
    val k = key(fid, id)
    dba.get(k).flatMap{
      case Some(x) => Right(decode(x))
      case None => Left(KeyNotFound)
    }

  private def delete(fid: Fid, id: String)(using dba: Dba): Either[Err, Unit] =
    dba.delete(key(fid, id)).void

  /**
   * Adds the entry to the container
   * Creates the container if it's absent
   * @param en entry to add (prev is ignored). If id is empty it will be generated
   */
  def add(en: A)(using dba: Dba): Either[Err, A] =
    get(en.fid).flatMap(_.cata(Right(_), create(en.fid))).flatMap{ (fd: Fd) =>
      ( if (en.id == empty)
          dba.nextid(en.fid) // generate ID if it is empty
        else
          get(en.fid, en.id).flatMap( // id of entry must be unique
            _.cata(_ => Left(EntryExists(key(en.fid, en.id))), Right(en.id))
          )
      ).map(id => en.copy(id=id, prev=fd.top)).flatMap{ en =>
        // add new entry with prev pointer
        _put(en).flatMap{ en =>
          // update feed's top
          update(fd, top=en.id).map(_ => en)
        }
      }
    }

  /**
   * Puts the entry to the container
   * If entry don't exists in containter create container and add it to the head
   * If entry exists in container, put it in the same place
   * @param en entry to put (prev is ignored)
   */
  def put(en: A)(using Dba): Either[Err, A] =
    get(en.fid, en.id).fold(
      l => Left(l),
      r => r.cata(x => _put(en.copy(x.prev)), add(en))
    )

  /** Iterates through container and return the stream of entries.
   *
   * Stream is FILO ordered (most recent is first).
   * @param from if specified then return entries after this entry
   */
  def all(fid: Fid)(using Dba): Either[Err, LazyList[Either[Err, A]]] =
    all(fid, from=None)

  def all(fid: Fid, from: Option[A])(using Dba): Either[Err, LazyList[Either[Err, A]]] =
    def _stream(id: String): LazyList[Either[Err, A]] =
      id match
        case `empty` => LazyList.empty
        case _ =>
          val en = getOrFail(fid, id)
          en match
            case Right(e) => LazyList.cons(en, _stream(e.prev))
            case _ => LazyList(en)
    from match
      case None => get(fid).map(_.cata(x => _stream(x.top), LazyList.empty))
      case Some(en) => Right(_stream(en.prev))

  def remove(_fid: Fid, _id: String)(using Dba): Either[Err, Unit] =
    // get entry to delete
    getOrFail(_fid, _id).flatMap{ en =>
      val id = en.id
      val fid = en.fid
      val prev = en.prev
      get(fid).flatMap(_.cata(Right(_), Left(KeyNotFound))).flatMap{ fd =>
        val top = fd.top
        ( if (id == top)
            // change top and decrement
            update(fd, top=prev)
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
end IdxHandler
