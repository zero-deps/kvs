package akka

import zd.proto.macrosapi.{messageCodecNums}

package object cluster {
  implicit val vcodec = messageCodecNums[Tuple2[String,Long]]('_1->1,'_2->2)
  implicit val vccodec = messageCodecNums[akka.cluster.VectorClock]('versions->1)
  val emptyVC = VectorClock()
}
