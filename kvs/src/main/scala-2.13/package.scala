package zd

package object kvs {
  // Don't change ever
  val empty = "empty_8fc62083-b0d1-49cc-899c-fbb9ab177241"

  type Res[A] = Either[Err, A]

  implicit class LeftExt[L,R](x: Left[L,R]) {
    def coerceRight[R2]: Either[L,R2] = x.asInstanceOf[Either[L,R2]]
  }
  implicit private class RightExt[L,R](x: Right[L,R]) {
    def coerceLeft[L2]: Either[L2,R] = x.asInstanceOf[Either[L2,R]]
  }
  implicit class EitherExt[L,R](x: Either[L,R]) {
    def leftMap[L2](f: L => L2): Either[L2,R] = x match {
      case y@Right(_) => y.coerceLeft
      case Left(l) => Left(f(l))
    }
    def recover(pf: PartialFunction[L,R]): Either[L,R] = x match {
      case Left(l) if pf isDefinedAt l => Right(pf(l))
      case _ => x
    }
  }
  implicit class OptionExt[A](x: Option[A]) {
    def cata[B](f: A => B, b: => B): B = x match {
      case Some(a) => f(a)
      case None => b
    }
  }
  implicit class BooleanExt(x: Boolean) {
    def fold[A](t: => A, f: => A): A = if (x) t else f
  }
  implicit class SeqEitherSequenceExt[A,B](xs: Seq[Either[A, B]]) {
    @annotation.tailrec private def _sequence(ys: Seq[Either[A, B]], acc: Vector[B]): Either[A, Vector[B]] = {
      ys.headOption match {
        case None => Right(acc)
        case Some(l@Left(_)) => l.coerceRight
        case Some(Right(z)) => _sequence(ys.tail, acc :+ z)
      }
    }
    def sequence: Either[A, Vector[B]] = _sequence(xs, Vector.empty)

    @annotation.tailrec private def _sequence_(ys: Seq[Either[A, B]]): Either[A, Unit] = {
      ys.headOption match {
        case None => Right(())
        case Some(l@Left(_)) => l.coerceRight
        case Some(Right(z)) => _sequence_(ys.tail)
      }
    }
    def sequence_ : Either[A, Unit] = _sequence_(xs)
  }
}
