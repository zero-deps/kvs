package kvs
package store

import concurrent.Future

import java.util.concurrent.ConcurrentHashMap
import zero.ext._, option._, either._
import zd.proto.Bytes
import net.jpountz.lz4._

object Mem {
  def apply(): Mem = new Mem
}

class Mem extends Dba with AutoCloseable {
  type DecompressedLength = Int
  private val db = new ConcurrentHashMap[Key, (DecompressedLength, Bytes)]
  private val lz4 = LZ4Factory.safeInstance()

  def get(key: Key): Res[Option[Bytes]] = {
    fromNullable(db.get(key)).map{ case (decompressedLength, compressed) =>
      val decompressor = lz4.fastDecompressor
      val restored = new Array[Byte](decompressedLength)
      decompressor.decompress(compressed.unsafeArray, 0, restored, 0, decompressedLength)
      Bytes.unsafeWrap(restored)
    }.right
  }

  def put(key: Key, value: Bytes): Res[Unit] = {
    val decompressedLength = value.length
    val compressor = lz4.highCompressor
    val maxCompressedLength = compressor.maxCompressedLength(decompressedLength)
    val compressed = new Array[Byte](maxCompressedLength)
    compressor.compress(value.unsafeArray, 0, decompressedLength, compressed, 0, maxCompressedLength)
    db.put(key, decompressedLength -> Bytes.unsafeWrap(compressed)).right.void
  }

  def delete(key: Key): Res[Unit] = {
    db.remove(key).right.void
  }

  def load(path: String): Res[Any] = ???
  def save(path: String): Res[String] = ???

  def onReady(): Future[Unit] = Future.successful(())
  def compact(): Unit = ()
  def close(): Unit = ()
}
