package mws

package object kvs {
  type Err = String
  type Res[T] = Either[Err,T]
}
