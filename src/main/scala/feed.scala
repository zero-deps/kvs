package kvs

import zero.ext._, option._, boolean._
import proto._, macrosapi._
import zio.{IO, ZIO}
import zio.stream.{Stream, ZStream}

import store.Dba

final case class Fd
  ( @N(1) head: Option[ElKey]
  , @N(2) length: Long
  , @N(3) removed: Long
  , @N(4) maxid: ElKey
  )

object Fd {
  val empty = new Fd(head=none, length=0, removed=0, maxid=ElKey(Bytes.empty))
}

final case class En
  ( @N(1) next: Option[ElKey]
  , @N(2) data: Bytes
  , @N(3) removed: Boolean=false
  )

/**
 * Abstract type entry handler
 * Since we don't know the exact type the pickler/unpickler still needs to be provided explicitly
 * [head] -->next--> [en] -->next--> [nothing]
 */
object feed {
  implicit val ekc = caseCodecAuto[ElKey]
  implicit val fdc = caseCodecAuto[Fd]
  implicit val enc = caseCodecAuto[En]

  object meta {
    def len(id: FdKey        )(implicit dba: Dba): IO[Err, Long] = get(id).map(_.cata(_.length, 0))
    def del(id: FdKey        )(implicit dba: Dba): IO[Err, Unit] = dba.del(id)
    def put(id: FdKey, el: Fd)(implicit dba: Dba): IO[Err, Unit] =
      for {
        p <- pickle(el)
        x <- dba.put(id, p)
      } yield x
    def get(id: FdKey        )(implicit dba: Dba): IO[Err, Option[Fd]] = 
      dba.get(id).flatMap{
        case Some(x) => unpickle[Fd](x).map(_.some)
        case None    => IO.succeed(none)
      }
  }

  private def _get(key: EnKey)(implicit dba: Dba): IO[Err, Option[En]] = {
    dba.get(key).flatMap{
      case Some(x) =>
        unpickle[En](x).map{
          case en if en.removed => none
          case en => en.some
        }
      case None => IO.succeed(none)
    }
  }

  /**
   * Puts the entry to the container
   * If entry don't exists in containter create container and add it to the head
   * If entry exists in container, put it in the same place
   */
  def put(key: EnKey, data: Bytes)(implicit dba: Dba): IO[Err, Unit] = {
    for {
      x  <- _get(key)
      y  <- x.cata(y =>
              for {
                p    <- pickle(En(next=y.next, data=data))
                x    <- dba.put(key, p)
              } yield x
            , for {
                fd1  <- meta.get(key.fid)
                fd   <- fd1.cata(IO.succeed(_), meta.put(key.fid, Fd.empty).map(_ => Fd.empty))
                en    = En(next=fd.head, data=data)
                maxid = max(key.id, fd.maxid)
                _    <- meta.put(key.fid, fd.copy(maxid=maxid)) // in case kvs will fail after adding the en
                p    <- pickle(en)
                x    <- dba.put(key, p)
                _    <- meta.put(key.fid, fd.copy(head=key.id.some, length=fd.length+1, maxid=maxid))
              } yield x
            )
    } yield y
  }

  def get(key: EnKey)(implicit dba: Dba): IO[Err, Option[Bytes]] = {
    _get(key).map(_.map(_.data))
  }

  def apply(key: EnKey)(implicit dba: Dba): IO[Err, Bytes] = {
    get(key).flatMap(_.cata(IO.succeed(_), IO.dieMessage("key is not exists")))
  }

  /**
   * Mark entry for removal. O(1) complexity.
   * @return true if marked for removal
   */
  def remove(key: EnKey)(implicit dba: Dba): IO[Err, Boolean] = {
    for {
      en1  <- _get(key)
      res  <- en1.cata(en =>
                for {
                  fd <- meta.get(key.fid).flatMap(_.cata(IO.succeed(_), IO.dieMessage("feed is not exists")))
                  _  <- meta.put(key.fid, fd.copy(length=fd.length-1, removed=fd.removed+1))
                  p  <- pickle(en.copy(removed=true))
                  _  <- dba.put(key, p)
                } yield true
              , IO.succeed(false)
              )
    } yield res
  }

  /**
   * Adds the entry to the container. Creates the container if it's absent.
   * ID will be generated.
   */
  def add(fid: FdKey, data: Bytes)(implicit dba: Dba): IO[Err, ElKey] = {
    for {
      fd1 <- meta.get(fid)
      fd <- fd1.cata(IO.succeed(_), meta.put(fid, Fd.empty).map(_ => Fd.empty))
      id = fd.maxid.increment()
      en = En(next=fd.head, data=data)
      _ <- meta.put(fid, fd.copy(maxid=id)) // in case kvs will fail after adding the en
      key = EnKey(fid, id)
      p <- pickle(en)
      _ <- dba.put(key, p)
      _ <- meta.put(fid, fd.copy(head=id.some, length=fd.length+1, maxid=id))
    } yield id
  }

  /**
   * Adds the entry to the container. Creates the container if it's absent.
   */
  def putIfAbsent(key: EnKey, data: Bytes)(implicit dba: Dba): IO[Err, Unit] = {
    for {
      en1 <- _get(key)
      _   <- en1.cata(_ => IO.fail(EntryExists(key)), IO.succeed(()))
      x   <- put(key, data)
    } yield x
  }

  def head(fid: FdKey)(implicit dba: Dba): IO[Err, Option[(ElKey, Bytes)]] = {
    all(fid).runHead
  }

  /* all items with removed starting from specified key */
  private def entries(fid: FdKey, start: ElKey)(implicit dba: Dba): Stream[Err, (ElKey, En)] = {
    Stream.
      unfoldM(start.some: Option[ElKey]){
        case None     => IO.succeed(none)
        case Some(id) =>
          for {
            bs <- dba(EnKey(fid, id))
            en <- unpickle[En](bs)
          } yield ((id->en)->en.next).some
      }
  }

  /* all items with removed starting from beggining */
  private def entries(fid: FdKey)(implicit dba: Dba): Stream[Err, (ElKey, En)] = {
    Stream.fromEffect(meta.get(fid)).flatMap{
      case None    => Stream.empty
      case Some(a) => a.head.cata(entries(fid, _), Stream.empty)
    }
  }

  /* all items without removed starting from specified key */
  private def entries_live(fid: FdKey, start: ElKey)(implicit dba: Dba): Stream[Err, (ElKey, En)] = {
    entries(fid, start).
      filterNot{ case (_,en) => en.removed }
  }

  /* all items without removed starting from beggining */
  private def entries_live(fid: FdKey)(implicit dba: Dba): Stream[Err, (ElKey, En)] = {
    entries(fid).
      filterNot{ case (_,en) => en.removed }
  }

  /* all data without removed from beggining */
  def all(fid: FdKey)(implicit dba: Dba): Stream[Err, (ElKey, Bytes)] = {
    entries_live(fid).map{ case (id,en) => id->en.data }
  }

  /* all data without removed from specified key */
  def all(fid: FdKey, start: ElKey)(implicit dba: Dba): Stream[Err, (ElKey, Bytes)] = {
    entries_live(fid, start).map{ case (id,en) => id->en.data }
  }

  /* delete all entries marked for removal, O(n) complexity */
  def cleanup(fid: FdKey)(implicit dba: Dba): IO[Err, Unit] = {
    meta.get(fid).flatMap{
      case None => IO.unit
      case Some(fd) =>
        for {
                /* remove from head */
          x  <- entries(fid).
                  takeWhile{ case (_,en) => en.removed }.
                  mapM{ case (id,en) => dba.del(EnKey(fid,id)).map(_=>en.next) }.
                  runLast
                /* fix head */
          _  <- x.cata(id => meta.put(fid, fd.copy(head=id)), IO.unit)
                /* remove in the middle */
          _  <- entries(fid).
                  /* zip with stream of last live entry before removed ones */
                  /* [1(live),2(removed),3(removed),4(live),5(removed)] becomes [1,1,1,4,4] */
                  zip(entries(fid).scan(none: Option[(ElKey,En)]){
                    case (None, (id,en)) => (id->en).some
                    case (   x, ( _,en)) if en.removed => x
                    case (   _, (id,en)) => (id->en).some
                  }.collect{ case Some(x)=>x }).
                  filter{ case (( _,en),_) => en.removed }.
                  mapM  { case ((id,en),(id2,en2)) =>
                    for {
                      /* change link */
                      p <- pickle(en2.copy(next=en.next))
                      _ <- dba.put(EnKey(fid,id2), p)
                      /* delete entry */
                      _ <- dba.del(EnKey(fid,id))
                    } yield ()
                  }.
                  runDrain
                /* fix removed/maxid */
          _  <- fix(fid, fd)
        } yield ()
    }
  }

  /* fix length, removed and maxid for feed */
  def fix(fid: FdKey, fd: Fd)(implicit dba: Dba): IO[Err, Unit] = {
    for {
      x  <- entries(fid).fold((0L,0L,ElKey(Bytes.empty))){
              case ((l,r,m),(id,en)) =>
                val l2 = en.removed.fold(l,l+1)
                val r2 = en.removed.fold(r+1,r)
                val m2 = max(m,id)
                (l2,r2,m2)
            }
      (l,r,m) = x
      _  <- meta.put(fid, fd.copy(length=l, removed=r, maxid=m))
    } yield ()
  }

  private def max(x: ElKey, y: ElKey): ElKey = {
    import java.util.Arrays
    if (Arrays.compare(x.bytes.unsafeArray, y.bytes.unsafeArray) > 0) x else y
  }
}
