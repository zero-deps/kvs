package zd.rng
  
extension [A,B](xs: Seq[Either[A, B]])
  @annotation.tailrec private def _sequence_(ys: Seq[Either[A, B]]): Either[A, Unit] =
    ys.headOption match
      case None => Right(())
      case Some(Left(e)) => Left(e)
      case Some(Right(z)) => _sequence_(ys.tail)
  inline def sequence_ : Either[A, Unit] = _sequence_(xs)