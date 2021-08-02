package zd
package dump

import akka.actor.{Actor, ActorLogging, Props}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Paths
import java.nio.file.StandardOpenOption.{READ, WRITE, CREATE}
import codec.*
import scala.util.Try
import proto.*

type Key = Array[Byte]
type Value = Array[Byte]

object DumpIO {
  def props(ioPath: String): Throwable Either Props = {
    Try(FileChannel.open(Paths.get(ioPath), READ, WRITE, CREATE).nn).toEither.map(channel => Props(new DumpIO(ioPath, channel)))
  }

  case object ReadNext
  final case class ReadNextRes(kv: Vector[(Key, Value)])
  final case object ReadNextLast

  final case class Put(kv: Vector[(Key, Value)])
  final case object PutDone
}

class DumpIO(ioPath: String, channel: FileChannel) extends Actor with ActorLogging {

  def receive = {
    case _: DumpIO.ReadNext.type =>
      val key = ByteBuffer.allocateDirect(4).nn
      val keyRead = channel.read(key)
      if (keyRead == 4) {
        val blockSize: Int = key.flip.asInstanceOf[ByteBuffer].getInt
        val value: Array[Byte] = new Array[Byte](blockSize)
        val valueRead: Int = channel.read(ByteBuffer.wrap(value))
        if (valueRead == blockSize) {
          val kv: Vector[(Key, Value)] = decode[DumpKV](value).kv.view.map(d => d.k -> d.v).to(Vector)
          sender ! DumpIO.ReadNextRes(kv)
        } else {
          log.error(s"failed to read dump io, blockSize=${blockSize}, valueRead=${valueRead}")
          sender ! DumpIO.ReadNextLast
        }
      } else if (keyRead == -1) {
        sender ! DumpIO.ReadNextLast
      } else {
        log.error(s"failed to read dump io, keyRead=${keyRead}")
        sender ! DumpIO.ReadNextLast
      }
    case msg: DumpIO.Put => 
      val data = encode(DumpKV(msg.kv.map(e => KV(e._1, e._2))))
      channel.write(ByteBuffer.allocateDirect(4).nn.putInt(data.size).nn.flip.nn.asInstanceOf[ByteBuffer])
      channel.write(ByteBuffer.wrap(data))
      sender ! DumpIO.PutDone
  }

  override def postStop(): Unit = {
    channel.close()
    super.postStop()
  }
}

object DumpIterate {
  def props(f: (Key, Value) => Unit): Props = Props(new DumpIterate(f))
}

class DumpIterate(f: (Key, Value) => Unit) extends Actor {
  def receive = {
    case msg: DumpIO.Put =>
      msg.kv.foreach(e => f(e._1, e._2))
      sender ! DumpIO.PutDone
  }
}
