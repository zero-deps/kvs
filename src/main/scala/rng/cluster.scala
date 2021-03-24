package akka

import proto.macrosapi.caseCodecNums

package object cluster {
  implicit val vcodec = caseCodecNums[(String,Long)]("_1"->1, "_2"->2)
  implicit val vccodec = caseCodecNums[akka.cluster.VectorClock]("versions"->1)
  val emptyVC = VectorClock()
}
