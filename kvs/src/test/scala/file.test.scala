package kvs
package file

import zio._, test._, Assertion._
import proto.Bytes

object FileSpec extends DefaultRunnableSpec {
  def spec = suite("FileSpec")(
    testM("size if absent") {
      assertM(flh.size(path2)           .run)(fails(equalTo(FileNotExists(path2))))
    }
  , testM("content if absent") {
      assertM(flh.stream(path2).runDrain.run)(fails(equalTo(FileNotExists(path2))))
    }
  , testM("delete if absent") {
      assertM(flh.delete(path2)         .run)(fails(equalTo(FileNotExists(path2))))
    }
  , testM("create if exists") {
      for {
        _ <- flh.create(path3)
        x <- flh.create(path3)          .run
      } yield assert(x)                      (fails(equalTo(FileAlreadyExists(path3))))
    }
  , testM("create/append/delete") {
      for {
        x1 <- flh.create(path)
        x2 <- flh.append(path, Bytes.unsafeWrap(Array(1, 2, 3, 4, 5, 6)))
        x3 <- flh.append(path, Bytes.unsafeWrap(Array(1, 2, 3, 4, 5, 6)))
        x4 <- flh.size(path)
        x5 <- flh.stream(path).runCollect
        x6 <- flh.delete(path)
      } yield assert(x1)(equalTo(File(count=0,size= 0,dir=false))) &&
              assert(x2)(equalTo(File(count=2,size= 6,dir=false))) &&
              assert(x3)(equalTo(File(count=4,size=12,dir=false))) &&
              assert(x4)(equalTo(12L))                             &&
              assert(x5)(equalTo(Chunk( Bytes.unsafeWrap(Array(1, 2, 3, 4, 5))
                                      , Bytes.unsafeWrap(Array(6            ))
                                      , Bytes.unsafeWrap(Array(1, 2, 3, 4, 5))
                                      , Bytes.unsafeWrap(Array(6            ))
                                      )))                          &&
              assert(x6)(equalTo(File(count=4,size=12,dir=false)))
    }
  )

  implicit val dba = store.Mem()
  val flh = new FileHandler { override val chunkLength = 5 }
  val dir   = FdKey(Bytes.unsafeWrap(Array[Byte](0)))
  val name1 = ElKey(Bytes.unsafeWrap(Array[Byte](1)))
  val name2 = ElKey(Bytes.unsafeWrap(Array[Byte](2)))
  val name3 = ElKey(Bytes.unsafeWrap(Array[Byte](3)))
  val path  = PathKey(dir, name1)
  val path2 = PathKey(dir, name2)
  val path3 = PathKey(dir, name3)
}
