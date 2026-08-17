package zd.kvs

import akka.actor.ExtendedActorSystem
import akka.serialization.BaseSerializer
import proto.*

final class ShardingSerializer(val system: ExtendedActorSystem) extends BaseSerializer:
  override val includeManifest: Boolean = false

  override def toBinary(value: AnyRef): Array[Byte] =
    value match
      case message: ShardingMessage => encode(message)
      case _ => throw IllegalArgumentException(s"${getClass.getName} can't serialize [$value]")

  override def fromBinary(data: Array[Byte], manifest: Option[Class[?]]): AnyRef =
    decode[ShardingMessage](data).asInstanceOf[AnyRef]
end ShardingSerializer
