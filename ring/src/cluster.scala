package org.apache.pekko

import proto.*

package object cluster {
  implicit val vcodec: MessageCodec[(String,Long)] = caseCodecNums[(String,Long)]("_1"->1, "_2"->2)
  implicit val vccodec: MessageCodec[org.apache.pekko.cluster.VectorClock] = caseCodecNums[org.apache.pekko.cluster.VectorClock]("versions"->1)
  val emptyVC = VectorClock()
}
