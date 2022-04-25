package kvs.feed

import kvs.rng.Dba
import proto.*
import zio.*, stream.*

/* Abstract type feed */
trait Feed:
  def all[A: Codec](fid: Fid, eid: Option[Eid]=None): Stream[Err, (Eid, A)]
  def get[A: Codec](fid: Fid, eid: Eid): IO[Err, Option[A]]
  def add[A: Codec](fid: Fid, a: A): IO[Err, Eid]
  def remove(fid: Fid, eid: Eid): IO[Err, Boolean]
  def cleanup(fid: Fid): IO[Err, Unit]
end Feed

def all[A: Codec](fid: Fid, eid: Option[Eid]=None): ZStream[Feed, Err, (Eid, A)] =
  ZStream.serviceWithStream(_.all(fid, eid))

def get[A: Codec](fid: Fid, eid: Eid): ZIO[Feed, Err, Option[A]] =
  ZIO.serviceWithZIO(_.get(fid, eid))

def add[A: Codec](fid: Fid, a: A): ZIO[Feed, Err, Eid] =
  ZIO.serviceWithZIO(_.add(fid, a))

def remove(fid: Fid, eid: Eid): ZIO[Feed, Err, Boolean] =
  ZIO.serviceWithZIO(_.remove(fid, eid))

def cleanup(fid: Fid): ZIO[Feed, Err, Unit] =
  ZIO.serviceWithZIO(_.cleanup(fid))

val live: URLayer[Dba, Feed] =
  ZLayer(
    for
      dba <- ZIO.service[Dba]
    yield
      new Feed:
        def all[A: Codec](fid: Fid, eid: Option[Eid]=None): Stream[Err, (Eid, A)] =
          eid.fold(ops.all(fid))(ops.all(fid, _))(dba).mapZIO{ case (k, a) =>
            for
              b <- unpickle(a)
            yield k -> b
          }

        def get[A: Codec](fid: Fid, eid: Eid): IO[Err, Option[A]] =
          for
            res <- ops.get(fid, eid)(dba)
            a <-
              (for
                b <- ZIO.fromOption(res)
                a <- unpickle[A](b)
              yield a).unsome
          yield a

        def add[A: Codec](fid: Fid, a: A): IO[Err, Eid] =
          for
            b <- pickle(a)
            eid <- ops.add(fid, b)(dba)
          yield eid

        def remove(fid: Fid, eid: Eid): IO[Err, Boolean] =
          ops.remove(fid, eid)(dba)

        def cleanup(fid: Fid): IO[Err, Unit] =
          ops.cleanup(fid)(dba)
  )
