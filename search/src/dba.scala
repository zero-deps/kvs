package kvs.search

import kvs.rng.{Dba, DbaErr}
import scala.util.Try
import zio.*

class DbaEff(dba: Dba):
  type K = String
  type V = Array[Byte]
  type Err = DbaErr | Throwable
  type R[A] = Either[Err, A]

  def put(key: K, value: V): R[Unit] = run(dba.put(stob(key), value))
  def get(key: K): R[Option[V]] = run(dba.get(stob(key)))
  def delete(key: K): R[Unit] = run(dba.delete(stob(key)))

  private def run[A](eff: IO[Err, A]): R[A] =
    Unsafe.unsafe(Runtime.default.unsafe.run(eff.either).toEither).flatten

  private inline def stob(s: String): Array[Byte] =
    s.getBytes("utf8").nn
