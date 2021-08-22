package kvs
package store

import java.util.concurrent.ConcurrentHashMap
import net.jpountz.lz4.*
import zio.*

object Mem {
  def apply(): Mem = new Mem
}

class Mem extends Dba with AutoCloseable {
  type DecompressedLength = Int
  private val db = new ConcurrentHashMap[Key, (DecompressedLength, Array[Byte])]
  private val lz4 = LZ4Factory.safeInstance()

  def get(key: Key): IO[Err, Option[Array[Byte]]] = {
    IO.effectTotal(Option(db.get(key)).map{ case (decompressedLength, compressed) =>
      val decompressor = lz4.fastDecompressor
      val restored = new Array[Byte](decompressedLength)
      decompressor.decompress(compressed, 0, restored, 0, decompressedLength)
      restored
    })
  }

  def put(key: Key, value: Array[Byte]): IO[Err, Unit] = {
    val decompressedLength = value.length
    val compressor = lz4.highCompressor
    val maxCompressedLength = compressor.maxCompressedLength(decompressedLength)
    val compressed = new Array[Byte](maxCompressedLength)
    compressor.compress(value, 0, decompressedLength, compressed, 0, maxCompressedLength)
    db.put(key, decompressedLength -> compressed)
    IO.succeed(())
  }

  def del(key: Key): IO[Err, Unit] = {
    db.remove(key)
    IO.succeed(())
  }

  def close(): Unit = ()
}
