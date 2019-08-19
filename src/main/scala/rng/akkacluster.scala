package akka

import zd.proto.macrosapi.{caseCodecNums}

package object cluster {
  implicit val vcodec = caseCodecNums[Tuple2[String,Long]](Symbol("_1")->1,Symbol("_2")->2)
  implicit val vccodec = caseCodecNums[akka.cluster.VectorClock](Symbol("versions")->1)
  val emptyVC = VectorClock()
}
