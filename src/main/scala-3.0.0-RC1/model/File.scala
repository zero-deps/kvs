package zd.kvs
package file

import proto.*

final case class File
  ( // name – unique value inside directory
    @N(1) name: String
    // count – number of chunks
  , @N(2) count: Int
    // size - size of file in bytes
  , @N(3) size: Long
    // true if directory
  , @N(4) dir: Boolean
  )
