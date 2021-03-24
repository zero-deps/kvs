package kvs
package search

import org.apache.lucene.store.IndexInput
import scala.annotation.tailrec
import java.io._
import proto.Bytes

class BytesIndexInput(resourceDescription: String, xs: Vector[Bytes], offset: Long, length: Long)
    extends IndexInput(resourceDescription) {

  def this(d: String, xs: Vector[Bytes]) = this(d, xs, 0, xs.foldLeft(0L)((acc, x) => acc + x.unsafeArray.length))

  private var open = true
  private var pos = offset

  override def close(): Unit = open = false
  override def getFilePointer(): Long = { ensureOpen(); pos - offset }
  override def length(): Long = { ensureOpen(); length }
  override def readByte(): Byte = {
    ensureOpen()
    @tailrec def loop(remaining: Vector[Bytes], p: Long): Byte = {
      remaining.headOption match {
        case Some(head) if p < head.length => head.unsafeArray(Math.toIntExact(p))
        case Some(head) => loop(remaining.tail, p-head.unsafeArray.length)
        case None => throw new EOFException
      }
    }
    val b = loop(xs, pos)
    pos += 1
    b
  }
  override def readBytes(ys: Array[Byte], ys_offset: Int, ys_len: Int): Unit = {
    ensureOpen()
    @tailrec def loop_copy(src: Bytes, remaining: Vector[Bytes], src_offset: Int, dst_offset: Int, len_remaining: Int): Unit = {
      val len1 = Math.min(len_remaining, src.unsafeArray.length-src_offset)
      System.arraycopy(src.unsafeArray, src_offset, ys, dst_offset, len1)
      if (len1 < len_remaining) {
        remaining.headOption match {
          case Some(head2) => loop_copy(head2, remaining.tail, 0, dst_offset+len1, len_remaining-len1)
          case None => throw new EOFException
        }
      }
    }
    @tailrec def loop_find(remaining: Vector[Bytes], p: Long): Unit = {
      remaining.headOption match {
        case Some(head) if p < head.length =>
          loop_copy(src=head, remaining=remaining.tail, src_offset=Math.toIntExact(p), dst_offset=ys_offset, len_remaining=ys_len)
        case Some(head) => loop_find(remaining.tail, p-head.length)
        case None => throw new EOFException
      }
    }
    loop_find(xs, pos)
    pos += ys_len
  }
  override def seek(p: Long): Unit = {
    ensureOpen()
    pos = p + offset
    if (p < 0 || p > length) throw new EOFException
  }
  override def slice(sliceDescription: String, o: Long, l: Long): IndexInput = {
    ensureOpen()
    new BytesIndexInput(sliceDescription, xs, offset+o, l)
  }
  
  private def ensureOpen(): Unit = {
    if (!open) throw new IOException("closed")
  }
}
