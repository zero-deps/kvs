package kvs.search

import proto.*
import scala.annotation.tailrec
import scala.collection.immutable.TreeSet

case class Fd
  ( @N(1) dirname: String
  , @N(2) head: Option[String]
  )

case class En
  ( @N(1) filename: String
  , @N(2) next: Option[String]
  )

object Files:
  given MessageCodec[Fd] = caseCodecAuto
  given MessageCodec[En] = caseCodecAuto

  def put(dirname: String)(using dba: DbaEff): Either[dba.Err, Fd] =
    put(Fd(dirname, head=None))

  def put(fd: Fd)(using dba: DbaEff): Either[dba.Err, Fd] =
    dba.put(fd.dirname, encode(fd)).map(_ => fd)
  
  def get(dirname: String)(using dba: DbaEff): Either[dba.Err, Option[Fd]] =
    val fd = Fd(dirname, head=None)
    dba.get(fd.dirname).map(_.map(decode))

  def delete(dirname: String)(using dba: DbaEff): Either[dba.Err, Unit] =
    dba.delete(dirname)

  private inline def key(dirname: String, filename: String): String = s"/search/files/$dirname/$filename"

  private def _put(dirname: String, en: En)(using dba: DbaEff): Either[dba.Err, En] =
    dba.put(key(dirname=dirname, filename=en.filename), encode(en)).map(_ => en)
  
  def get(dirname: String, filename: String)(using dba: DbaEff): Either[dba.Err, Option[En]] =
    dba.get(key(dirname=dirname, filename=filename)).map(_.map(decode))
  
  private def getOrFail[E](dirname: String, filename: String, err: => E)(using dba: DbaEff): Either[dba.Err | E, En] =
    given CanEqual[None.type, Option[dba.V]] = CanEqual.derived
    val k = key(dirname=dirname, filename=filename)
    dba.get(k).flatMap{
      case Some(x) => Right(decode(x))
      case None => Left(err)
    }

  private def delete(dirname: String, filename: String)(using dba: DbaEff): Either[dba.Err, Unit] =
    dba.delete(key(dirname=dirname, filename=filename))

  /**
   * Adds the entry to the container
   * Creates the container if it's absent
   */
  def add(dirname: String, filename: String)(using dba: DbaEff): Either[dba.Err | FileExists.type, En] =
    get(dirname).flatMap(_.fold(put(dirname))(Right(_))).flatMap{ (fd: Fd) =>
      (get(dirname=dirname, filename=filename).flatMap( // id of entry must be unique
        _.fold(Right(()))(_ => Left(FileExists))
      ): Either[dba.Err | FileExists.type, Unit])
      .map(_ => En(filename, next=fd.head)).flatMap{ en =>
        // add new entry with next pointer
        _put(dirname, en).flatMap{ en =>
          // update feed's head
          put(fd.copy(head=Some(filename))).map(_ => en)
        }
      }
    }

  def all(dirname: String)(using dba: DbaEff): Either[dba.Err | BrokenListing.type, Array[String | Null]] =
    @tailrec
    def loop(id: Option[String], acc: TreeSet[String]): Either[dba.Err | BrokenListing.type, Array[String | Null]] =
      id match
        case None => Right(acc.toArray)
        case Some(id) =>
          val en = getOrFail(dirname, id, BrokenListing)
          en match
            case Right(e) => loop(e.next, acc + e.filename)
            case Left(e) => Left(e)
    get(dirname).flatMap(_.fold(Right(Array.empty[String | Null]))(x => loop(x.head, TreeSet.empty)))

  def remove(dirname: String, filename: String)(using dba: DbaEff): Either[dba.Err | FileNotExists.type | BrokenListing.type, Unit] =
    for
      // get entry to delete
      en <- getOrFail(dirname, filename, FileNotExists)
      fdOpt <- get(dirname)
      _ <-
        fdOpt match
          case None =>
            // tangling en
            delete(dirname, filename)
          case Some(fd) =>
            (fd.head match
              case None => Right(())
              case Some(head) =>
                for
                  _ <-
                    if filename == head then
                      put(fd.copy(head=en.next))
                    else
                      @tailrec
                      def loop(id: Option[String]): Either[dba.Err | BrokenListing.type, En] =
                        id match
                          case None => Left(BrokenListing)
                          case Some(id) =>
                            val en = getOrFail(dirname, id, BrokenListing)
                            en match
                              case Right(e) if e.next == Some(filename) => Right(e)
                              case Right(e) => loop(e.next)
                              case Left(e) => Left(e)
                      (for
                        // find entry which points to this one (next)
                        next <- loop(Some(head))
                        // change link
                        _ <- _put(dirname, next.copy(next=en.next))
                      yield ()): Either[dba.Err | FileNotExists.type | BrokenListing.type, Unit]
                  _ <- delete(dirname, filename)
                yield ()): Either[dba.Err | FileNotExists.type | BrokenListing.type, Unit]
    yield ()

  given CanEqual[None.type, Option[Fd]] = CanEqual.derived

end Files
