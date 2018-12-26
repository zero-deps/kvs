package mws.rng

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.google.protobuf.{ByteString}
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit
import leveldbjnr._
import mws.kvs.LeveldbOps
import mws.rng.data.{Data}
import mws.rng.data_dump.{DumpKV, KV}
import mws.rng.msg_dump.{DumpPut, DumpGet, DumpEn, DumpGetBucketData, DumpBucketData, DumpBucketDataItem}
import mws.rng.store._
import scala.annotation.tailrec
import scala.collection.immutable.{SortedMap, SortedSet}
import scala.concurrent.{Await}
import scala.util.Try
import scalaz.Scalaz._

object DumpIO {
  def props(ioPath: String): Props = Props(new DumpIO(ioPath))

  case object ReadNext
  case class ReadNextRes(kv: Seq[(ByteString, ByteString)], last: Boolean)

  case class Put(kv: Seq[Data])
  case object PutDone
  case object PutClose
}

class DumpIO(ioPath: String) extends Actor with ActorLogging {
  import java.nio.ByteBuffer
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption.{READ, WRITE, CREATE}
  val channel: FileChannel = FileChannel.open(Paths.get(ioPath), READ, WRITE, CREATE)
  def receive = {
    case DumpIO.ReadNext =>
      val key = ByteBuffer.allocateDirect(4)
      val keyRead = channel.read(key)
      if (keyRead == 4) {
        val blockSize: Int = key.flip.asInstanceOf[ByteBuffer].getInt
        val value: Array[Byte] = new Array[Byte](blockSize)
        val valueRead: Int = channel.read(ByteBuffer.wrap(value))
        if (valueRead == blockSize) {
          val kv = DumpKV.parseFrom(value).kv.map(d => d.k -> d.v)
          sender ! DumpIO.ReadNextRes(kv, false)
        } else {
          log.error(s"failed to read dump io, blockSize=${blockSize}, valueRead=${valueRead}")
          sender ! DumpIO.ReadNextRes(Seq.empty, true)
        }
      } else if (keyRead == -1) {
        sender ! DumpIO.ReadNextRes(Seq.empty, true)
      } else {
        log.error(s"failed to read dump io, keyRead=${keyRead}")
        sender ! DumpIO.ReadNextRes(Seq.empty, true)
      }
    case msg: DumpIO.Put => 
      val data = DumpKV(msg.kv.map(e => KV(e.key, e.value))).toByteArray
      channel.write(ByteBuffer.allocateDirect(4).putInt(data.size).flip.asInstanceOf[ByteBuffer])
      channel.write(ByteBuffer.wrap(data))
      sender ! DumpIO.PutDone
    case DumpIO.PutDone => sender ! DumpIO.PutDone
    case DumpIO.PutClose => context.stop(self)
  }
  override def postStop(): Unit = {
    channel.close()
    super.postStop()
  }
}
