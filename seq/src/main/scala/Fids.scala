package kvs.seq

sealed trait ListFid[Fid, Key, A]
object ListFid {
  def apply[Fid, Key, A]() = new ListFid[Fid, Key, A] {}
}

sealed trait ArrayFid[Fid, A] {
  val size: Long
}
object ArrayFid {
  def apply[Fid, A](size: Long) = {
    val arraySize = size
    new ArrayFid[Fid, A] {
      val size: Long = arraySize
    }
  }
}
