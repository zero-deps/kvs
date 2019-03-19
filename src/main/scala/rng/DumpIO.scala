package mws.rng

import akka.actor.{Actor, ActorLogging, Props}
import com.google.protobuf.{ByteString}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Paths
import java.nio.file.StandardOpenOption.{READ, WRITE, CREATE}
import mws.rng.data.{Data}
import mws.rng.data_dump.{DumpKV, KV}
import scala.collection.{breakOut}
import scala.util.Try

object DumpIO {
  def props(ioPath: String): Throwable Either Props = {
    Try(FileChannel.open(Paths.get(ioPath), READ, WRITE, CREATE)).toEither.map(channel => Props(new DumpIO(ioPath, channel)))
  }

  final case object ReadNext
  final case class ReadNextRes(kv: Vector[(ByteString, ByteString)], last: Boolean)

  final case class Put(kv: Vector[Data])
  final case class PutDone(path: String)
}

class DumpIO(ioPath: String, channel: FileChannel) extends Actor with ActorLogging {

  def receive = {
    case DumpIO.ReadNext =>
      val key = ByteBuffer.allocateDirect(4)
      val keyRead = channel.read(key)
      if (keyRead == 4) {
        val blockSize: Int = key.flip.asInstanceOf[ByteBuffer].getInt
        val value: Array[Byte] = new Array[Byte](blockSize)
        val valueRead: Int = channel.read(ByteBuffer.wrap(value))
        if (valueRead == blockSize) {
          val kv: Vector[(ByteString, ByteString)] = DumpKV.parseFrom(value).kv.map(d => d.k -> d.v)(breakOut)
          sender ! DumpIO.ReadNextRes(kv, false)
        } else {
          log.error(s"failed to read dump io, blockSize=${blockSize}, valueRead=${valueRead}")
          sender ! DumpIO.ReadNextRes(Vector.empty, true)
        }
      } else if (keyRead == -1) {
        sender ! DumpIO.ReadNextRes(Vector.empty, true)
      } else {
        log.error(s"failed to read dump io, keyRead=${keyRead}")
        sender ! DumpIO.ReadNextRes(Vector.empty, true)
      }
    case msg: DumpIO.Put => 
      val data = DumpKV(msg.kv.map(e => KV(e.key, e.value))).toByteArray
      channel.write(ByteBuffer.allocateDirect(4).putInt(data.size).flip.asInstanceOf[ByteBuffer])
      channel.write(ByteBuffer.wrap(data))
      sender ! DumpIO.PutDone(ioPath)
    case x: DumpIO.PutDone =>
      sender ! x
  }

  override def postStop(): Unit = {
    channel.close()
    super.postStop()
  }
}
