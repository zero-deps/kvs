package kvs
package store

import java.util.concurrent.ConcurrentHashMap
import zero.ext._, option._
import zd.proto.Bytes
import net.jpountz.lz4._
import zio._

object Mem {
  def apply(): Mem = new Mem
}

class Mem extends Dba with AutoCloseable {
  type DecompressedLength = Int
  private val db = new ConcurrentHashMap[Key, (DecompressedLength, Bytes)]
  private val lz4 = LZ4Factory.safeInstance()

  def get(key: Key): KUIO[Option[Bytes]] = {
    IO.effectTotal(fromNullable(db.get(key)).map{ case (decompressedLength, compressed) =>
      val decompressor = lz4.fastDecompressor
      val restored = new Array[Byte](decompressedLength)
      decompressor.decompress(compressed.unsafeArray, 0, restored, 0, decompressedLength)
      Bytes.unsafeWrap(restored)
    })
  }

  def put(key: Key, value: Bytes): KUIO[Unit] = {
    val decompressedLength = value.length
    val compressor = lz4.highCompressor
    val maxCompressedLength = compressor.maxCompressedLength(decompressedLength)
    val compressed = new Array[Byte](maxCompressedLength)
    compressor.compress(value.unsafeArray, 0, decompressedLength, compressed, 0, maxCompressedLength)
    db.put(key, decompressedLength -> Bytes.unsafeWrap(compressed))
    IO.succeed(())
  }

  def del(key: Key): KUIO[Unit] = {
    db.remove(key)
    IO.succeed(())
  }

  def close(): Unit = ()
}
