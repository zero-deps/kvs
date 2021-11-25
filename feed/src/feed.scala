package kvs.feed

import akka.actor.{Actor, ActorLogging, Props}
import zio.*, stream.*

import kvs.rng.{ActorSystem, Dba}

import proto.*

/* Abstract type feed */

type Feed = Has[Feed.Service]

type Codec[A] = MessageCodec[A]

object Feed:
  trait Service:
    def all[A: Codec](fid: Fid, eid: Option[Eid]=None): Stream[Err, (Eid, A)]
    def get[A: Codec](fid: Fid, eid: Eid): IO[Err, Option[A]]
    def add[A: Codec](fid: Fid, a: A): IO[Err, Eid]
    def remove(fid: Fid, eid: Eid): IO[Err, Boolean]
    def cleanup(fid: Fid): IO[Err, Unit]
  end Service

  val live: RLayer[ActorSystem & Dba, Feed] = ZLayer.fromEffect{
    for
      dba <- ZIO.service[Dba.Service]
      as <- ZIO.service[ActorSystem.Service]
    yield
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
end Feed

object Shard:
  enum Msg:
    case Add(fid: Fid, data: Data)
    case Remove(fid: Fid, eid: Eid)
    case Cleanup(fid: Fid)
  
  case class Response(x: Either[Err, Unit])
  case class Added(x: Either[Err, Eid])
  case class Removed(x: Either[Err, Boolean])

end Shard

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
