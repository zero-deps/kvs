package kvs
package store

import proto.Bytes
import zio.IO

/**
 * Database Application Interface.
 */
trait Dba { _: AutoCloseable =>
  def get(key: Key): IO[Err, Option[Bytes]]
  def apply(key: Key): IO[Err, Bytes] = get(key).flatMap{
    case None     => IO.dieMessage("storage is corrupted")
    case Some(bs) => IO.succeed(bs)
  }
  def put(key: Key, value: Bytes): IO[Err, Unit]
  def del(key: Key): IO[Err, Unit]
}

sealed trait DbaConf
case class  RngConf(conf: Rng.Conf = Rng.Conf()) extends DbaConf
case class  RksConf(conf: Rks.Conf = Rks.Conf()) extends DbaConf
case object MemConf                              extends DbaConf
case object FsConf                               extends DbaConf
case object SqlConf                              extends DbaConf
