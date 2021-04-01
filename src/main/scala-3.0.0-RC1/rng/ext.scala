package zd.rng

extension [L,R,L2,R2](x: Left[L,R])
  inline def coerceRight: Either[L,R2] = x.asInstanceOf[Either[L,R2]]
  
extension [A,B](xs: Seq[Either[A, B]])
  @annotation.tailrec private def _sequence_(ys: Seq[Either[A, B]]): Either[A, Unit] =
    ys.headOption match
      case None => Right(())
      case Some(l@Left(_)) => l.coerceRight
      case Some(Right(z)) => _sequence_(ys.tail)
  inline def sequence_ : Either[A, Unit] = _sequence_(xs)