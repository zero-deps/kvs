package kvs

import zd.rng.*
import zio.*

/* Database API */
trait Dba:
  def put(key: Array[Byte], value: Array[Byte]): UIO[Unit]
  def get(key: Array[Byte]): UIO[Option[Array[Byte]]]
  def delete(key: Array[Byte]): UIO[Unit]

  def put(key: String, value: Array[Byte]): UIO[Unit] = put(key.getBytes("utf8").nn, value)
  def get(key: String): UIO[Option[Array[Byte]]] = get(key.getBytes("utf8").nn)
  def delete(key: String): UIO[Unit] = delete(key.getBytes("utf8").nn)
end Dba

type DbaErr = AckQuorumFailed | AckTimeoutFailed
