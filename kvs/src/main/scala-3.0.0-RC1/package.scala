package zd

package object kvs {
  // Don't change ever
  val empty = "empty_8fc62083-b0d1-49cc-899c-fbb9ab177241"

  type Res[A] = Either[Err, A]

  given noneCanEqual[A]: CanEqual[None.type, A] = CanEqual.derived
}
