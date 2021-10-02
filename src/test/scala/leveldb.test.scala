package leveldbjnr

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.*

class LeveldbTest extends AnyFreeSpec with Matchers with EitherValues {

  var leveldb: LevelDb | Null = null
  val path = "leveldb_test"
  val ro = ReadOpts()
  val wo = WriteOpts()

  "leveldb" - {
    "version" in {
      LevelDb.version should be ((1,20))
    }
    "destroy" in {
      LevelDb.destroy(path)
    }
    "create" in {
      leveldb = LevelDb.open(path).fold(l => throw l, r => r)
    }
    "no value" in {
      leveldb.nn.get(Array(1,2,3), ro) should be (Right(None))
    }
    "put" in {
      leveldb.nn.put(Array(1,2,3), Array(11,22,33), wo) should be (Right(()))
    }
    "read" in {
      leveldb.nn.get(Array(1,2,3), ro).map(_.map(_.toList)) should be (Right(Some(List(11,22,33))))
    }
    "delete" in {
      leveldb.nn.delete(Array(1,2,3), wo) should be (Right(()))
    }
    "read2" in {
      leveldb.nn.get(Array(1,2,3), ro) should be (Right(None))
    }
    "close & destroy" in {
      leveldb.nn.close()
      wo.close()
      ro.close()
      LevelDb.destroy(path) should be (Right(()))
    }
  }
}
