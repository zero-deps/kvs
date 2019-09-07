package zd

import scala.collection.immutable.ArraySeq

package object kvs {
  type Res[A] = Either[Err, A]

  implicit class FdIdExt(fd: en.FdId) {
    def +:(data: ArraySeq[Byte])(implicit kvs: Kvs): Res[en.En] = en.EnHandler.prepend(fd.id, data)(kvs.dba)
    def +:(id_data: (String, ArraySeq[Byte]))(implicit kvs: Kvs): Res[en.En] = en.EnHandler.prepend(fd.id, id_data._1, id_data._2)(kvs.dba)
  }
}
