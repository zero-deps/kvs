package zd.kvs

extension [A,B,U](x: Option[A])
  inline def cata(f: A => B, b: => B): B = x.fold(b)(f)

extension [A](x: Boolean)
  inline def fold(t: => A, f: => A): A = if x then t else f

extension [A,B](xs: Seq[Either[A, B]])
  @annotation.tailrec private def _sequence(ys: Seq[Either[A, B]], acc: Vector[B]): Either[A, Vector[B]] =
    ys.headOption match
      case None => Right(acc)
      case Some(Left(e)) => Left(e)
      case Some(Right(z)) => _sequence(ys.tail, acc :+ z)
  inline def sequence: Either[A, Vector[B]] = _sequence(xs, Vector.empty)

  @annotation.tailrec private def _sequence_(ys: Seq[Either[A, B]]): Either[A, Unit] =
    ys.headOption match
      case None => Right(())
      case Some(Left(e)) => Left(e)
      case Some(Right(z)) => _sequence_(ys.tail)
  inline def sequence_ : Either[A, Unit] = _sequence_(xs)

extension [L,R,U,L2,R2](x: Either[L,R])
  inline def leftMap(f: L => L2): Either[L2,R] = x match
    case Right(a) => Right(a)
    case y@Left(l) => Left(f(l))
  inline def recover(pf: PartialFunction[L,R]): Either[L,R] = x match
    case Left(l) if pf isDefinedAt l => Right(pf(l))
    case _ => x
  inline def void: Either[L, Unit] = x.map(_ => unit)

inline def unit: Unit = ()
