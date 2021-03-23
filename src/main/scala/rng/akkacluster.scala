package akka

import zd.proto.api._
import zd.proto.macrosapi.{caseCodecNums}

package object cluster {
  implicit val vcodec: MessageCodec[(String, Long)] = caseCodecNums[Tuple2[String,Long]]("_1"->1,"_2"->2)
  implicit val vccodec: MessageCodec[akka.cluster.VectorClock] = caseCodecNums[akka.cluster.VectorClock]("versions"->1)
  val emptyVC = VectorClock()
}
