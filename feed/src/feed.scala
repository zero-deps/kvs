package kvs.feed

import kvs.rng.Dba
import proto.*
import zio.*, stream.*

/* Abstract type feed */
type Feed = Has[Service]

trait Service:
  def all[A: Codec](fid: Fid, eid: Option[Eid]=None): Stream[Err, (Eid, A)]
  def get[A: Codec](fid: Fid, eid: Eid): IO[Err, Option[A]]
  def add[A: Codec](fid: Fid, a: A): IO[Err, Eid]
  def remove(fid: Fid, eid: Eid): IO[Err, Boolean]
  def cleanup(fid: Fid): IO[Err, Unit]
end Service

val live: URLayer[Dba, Feed] =
  ZLayer.fromFunction{ dba1 =>
    val dba = dba1.get
    new Service:
      def all[A: Codec](fid: Fid, eid: Option[Eid]=None): Stream[Err, (Eid, A)] =
        eid.fold(ops.all(fid))(ops.all(fid, _))(dba).mapM{ case (k, a) =>
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
            yield a).optional
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
  }

def all[A: Codec](fid: Fid, eid: Option[Eid]=None): ZStream[Feed, Err, (Eid, A)] =
  ZStream.accessStream(_.get.all(fid, eid))

def get[A: Codec](fid: Fid, eid: Eid): ZIO[Feed, Err, Option[A]] =
  ZIO.accessM(_.get.get(fid, eid))

def add[A: Codec](fid: Fid, a: A): ZIO[Feed, Err, Eid] =
  ZIO.accessM(_.get.add(fid, a))

def remove(fid: Fid, eid: Eid): ZIO[Feed, Err, Boolean] =
  ZIO.accessM(_.get.remove(fid, eid))

def cleanup(fid: Fid): ZIO[Feed, Err, Unit] =
  ZIO.accessM(_.get.cleanup(fid))
