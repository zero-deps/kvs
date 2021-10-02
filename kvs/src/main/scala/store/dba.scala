package kvs
package store

import zio.IO

/**
 * Database Application Interface.
 */
trait Dba { this: AutoCloseable =>
  def get(key: Key): IO[Err, Option[Array[Byte]]]
  def apply(key: Key): IO[Err, Array[Byte]] = get(key).flatMap{
    case None     => IO.dieMessage("storage is corrupted")
    case Some(bs) => IO.succeed(bs)
  }
  def put(key: Key, value: Array[Byte]): IO[Err, Unit]
  def del(key: Key): IO[Err, Unit]
}

sealed trait DbaConf
case class  RngConf(conf: Rng.Conf = Rng.Conf()) extends DbaConf
