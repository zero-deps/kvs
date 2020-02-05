/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package zd.kvs.search

import java.io.{EOFException, IOException, UncheckedIOException}
import java.util.Locale
import org.apache.lucene.store.{IndexInput, RandomAccessInput}

/**
 * A {//link IndexInput} backed by a byte array.
 * 
 * //lucene.experimental
 * //deprecated Will be removed in future Lucene versions. Use byte buffer backed index inputs instead.
 */
//@Deprecated
class ByteArrayIndexInput(
    description: String
  , private var bytes: Array[Byte]
  , offset: Int
  , length: Int
  ) extends IndexInput(description) with RandomAccessInput {

  private var pos: Int = offset

  def this(description: String, bytes: Array[Byte]) =
    this(description, bytes, 0, bytes.length)

  def getFilePointer(): Long = (pos - offset).toLong
  
  override def length(): Long = length.toLong

  @throws(classOf[EOFException])
  def seek(pos: Long): Unit = {
    val newPos: Int = Math.toIntExact(pos + offset)
    try {
      if (pos < 0 || pos > length) {
        throw new EOFException
      }
    }finally this.pos = newPos
  }

  override def readShort(): Short =
    (((bytes({ pos += 1; pos - 1 }) & 0xFF) << 8) | (bytes({
      pos += 1; pos - 1
    }) & 0xFF)).toShort

  override def readInt(): Int =
    ((bytes({ pos += 1; pos - 1 }) & 0xFF) << 24) | ((bytes({
      pos += 1; pos - 1
    }) & 0xFF) << 16) |
      ((bytes({ pos += 1; pos - 1 }) & 0xFF) << 8) |
      (bytes({ pos += 1; pos - 1 }) & 0xFF)

  override def readLong(): Long = {
    val i1: Int = ((bytes({ pos += 1; pos - 1 }) & 0xff) << 24) | ((bytes({
      pos += 1; pos - 1
    }) & 0xff) << 16) |
        ((bytes({ pos += 1; pos - 1 }) & 0xff) << 8) |
        (bytes({ pos += 1; pos - 1 }) & 0xff)
    val i2: Int = ((bytes({ pos += 1; pos - 1 }) & 0xff) << 24) | ((bytes({
      pos += 1; pos - 1
    }) & 0xff) << 16) |
        ((bytes({ pos += 1; pos - 1 }) & 0xff) << 8) |
        (bytes({ pos += 1; pos - 1 }) & 0xff)
    (i1.toLong << 32) | (i2 & 0xFFFFFFFFL)
  }

  override def readVInt(): Int = {
    var b: Byte = bytes({ pos += 1; pos - 1 })
    if (b >= 0) b.toInt
    else {
      var i: Int = b & 0x7F
      b = bytes({ pos += 1; pos - 1 })
      i |= (b & 0x7F) << 7
      if (b >= 0) i
      else {
        b = bytes({ pos += 1; pos - 1 })
        i |= (b & 0x7F) << 14
        if (b >= 0) i
        else {
          b = bytes({ pos += 1; pos - 1 })
          i |= (b & 0x7F) << 21
          if (b >= 0) i
          else {
            b = bytes({ pos += 1; pos - 1 })
            i |= (b & 0x0F) << 28
            if ((b & 0xF0) == 0) i
            else throw new RuntimeException("Invalid vInt detected (too many bits)")
          }
        }
      }
    }
  }

  override def readVLong(): Long = {
    var b: Byte = bytes({ pos += 1; pos - 1 })
    if (b >= 0) b.toLong
    else {
      var i: Long = b & 0x7FL
      b = bytes({ pos += 1; pos - 1 })
      i |= (b & 0x7FL) << 7
      if (b >= 0) i
      else {
        b = bytes({ pos += 1; pos - 1 })
        i |= (b & 0x7FL) << 14
        if (b >= 0) i
        else {
          b = bytes({ pos += 1; pos - 1 })
          i |= (b & 0x7FL) << 21
          if (b >= 0) i
          else {
            b = bytes({ pos += 1; pos - 1 })
            i |= (b & 0x7FL) << 28
            if (b >= 0) i
            else {
              b = bytes({ pos += 1; pos - 1 })
              i |= (b & 0x7FL) << 35
              if (b >= 0) i
              else {
                b = bytes({ pos += 1; pos - 1 })
                i |= (b & 0x7FL) << 42
                if (b >= 0) i
                else {
                  b = bytes({ pos += 1; pos - 1 })
                  i |= (b & 0x7FL) << 49
                  if (b >= 0) i
                  else {
                    b = bytes({ pos += 1; pos - 1 })
                    i |= (b & 0x7FL) << 56
                    if (b >= 0) i
                    else throw new RuntimeException("Invalid vLong detected (negative values disallowed)")
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  // NOTE: AIOOBE not EOF if you read too much
  override def readByte(): Byte = bytes({ pos += 1; pos - 1 })

  // NOTE: AIOOBE not EOF if you read too much
  override def readBytes(b: Array[Byte], offset: Int, len: Int): Unit = {
    System.arraycopy(bytes, pos, b, offset, len)
    pos += len
  }

  override def close(): Unit = {
    bytes = null
  }

  override def clone(): IndexInput = {
    val s: ByteArrayIndexInput = slice("(cloned)" + toString, 0, length())
    try {
      s.seek(getFilePointer)
    } catch {
      case e: EOFException => throw new UncheckedIOException(e)
    }
    s
  }

  def slice(sliceDescription: String, o: Long, l: Long): ByteArrayIndexInput = {
    if (o < 0 || l < 0 || o + l > length) {
      throw new IllegalArgumentException(String.format(Locale.ROOT, "slice(offset=%s, length=%s) is out of bounds: %s", o, l, this))
    }
    new ByteArrayIndexInput(sliceDescription, bytes, Math.toIntExact(offset + o), Math.toIntExact(l))
  }

  @throws(classOf[IOException])
  override def readByte(pos: Long): Byte = {
    bytes(Math.toIntExact(offset + pos))
  }

  @throws(classOf[IOException])
  override def readShort(pos: Long): Short = {
    val i: Int = Math.toIntExact(offset + pos)
    (((bytes(i) & 0xFF) << 8) | (bytes(i + 1) & 0xFF)).toShort
  }

  @throws(classOf[IOException])
  override def readInt(pos: Long): Int = {
    val i: Int = Math.toIntExact(offset + pos)
    ((bytes(i) & 0xFF) << 24) | ((bytes(i + 1) & 0xFF) << 16) |
      ((bytes(i + 2) & 0xFF) << 8) |
      (bytes(i + 3) & 0xFF)
  }

  @throws(classOf[IOException])
  override def readLong(pos: Long): Long = {
    (readInt(pos).toLong << 32) | (readInt(pos + 4) & 0xFFFFFFFFL)
  }
}