package mws.kvs
package store

import org.scalatest.{FreeSpecLike,Matchers,EitherValues}
import Memory._

class MemoryTest extends FreeSpecLike with Matchers with EitherValues {
  "Memory DBA should" - {
    val m: Dba = Memory(null)
    "be empty at creation" in {
      m.get("k1").left.value should be (not_found)
    }
    "save successfully value" in {
      m.put("k1", "v1".getBytes).right.value should be ("v1".getBytes)
    }
    "retrieve saved value" in {
      m.get("k1").right.value should be ("v1".getBytes)
    }
    "replace value" in {
      m.put("k1", "v2".getBytes) should be ('right)
      m.get("k1").right.value should be ("v2".getBytes)
    }
    "delete value" in {
      m.delete("k1").right.value should be ("v2".getBytes)
    }
    "and value is unavailable" in {
      m.get("k1") should be ('left)
    }
  }
}
