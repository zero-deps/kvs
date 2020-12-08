package kvs
package store

import zd.proto.Bytes
import zio._

/**
 * Database Application Interface.
 */
trait Dba { _: AutoCloseable =>
  def get(key: Key): KIO[Option[Bytes]]
  def apply(key: Key): KIO[Bytes] = get(key).flatMap{
    case None     => IO.dieMessage("storage is corrupted")
    case Some(bs) => IO.succeed(bs)
  }
  def put(key: Key, value: Bytes): KIO[Unit]
  def del(key: Key): KIO[Unit]
}

sealed trait DbaConf
case class  RngConf(conf: Rng.Conf = Rng.Conf()) extends DbaConf
case class  RksConf(conf: Rks.Conf = Rks.Conf()) extends DbaConf
case object MemConf                              extends DbaConf
case object FsConf                               extends DbaConf
case object SqlConf                              extends DbaConf
