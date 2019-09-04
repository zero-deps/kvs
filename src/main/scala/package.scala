package zd

package object kvs {
  type Res[A] = Either[Err, A]
}
