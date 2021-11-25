package akka

import proto.*

package object cluster {
  implicit val vcodec: MessageCodec[(String,Long)] = caseCodecNums[(String,Long)]("_1"->1, "_2"->2)
  implicit val vccodec: MessageCodec[akka.cluster.VectorClock] = caseCodecNums[akka.cluster.VectorClock]("versions"->1)
  val emptyVC = VectorClock()
}
