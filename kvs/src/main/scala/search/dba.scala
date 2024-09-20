package kvs
package search

import zio.*

class DbaEff(dba: Dba):
  type K = String
  type V = Array[Byte]
  type Err = DbaErr | Throwable
  type R[A] = Either[Err, A]

  def put(key: K, value: V): R[Unit] = run(dba.put(key, value))
  def get(key: K): R[Option[V]] = run(dba.get(key))
  def delete(key: K): R[Unit] = run(dba.delete(key))

  private def run[A](eff: IO[Err, A]): R[A] =
    Unsafe.unsafely(Runtime.default.unsafe.run(eff.either).toEither).flatten
end DbaEff
