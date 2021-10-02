package zd.rks

import akka.actor.{Actor, ActorLogging, ActorRef, Props, PoisonPill}
import java.time.format.{DateTimeFormatter}
import java.time.{LocalDateTime}
import org.rocksdb.*
import zd.dump.{DumpIO}

object DumpProcessor {
  def props(db: RocksDB): Props = Props(new DumpProcessor(db))

  case class Load(path: String)
  case class Save(path: String)
}

class DumpProcessor(db: RocksDB) extends Actor with ActorLogging {
  def receive = {
    case DumpProcessor.Load(path) =>
      log.info(s"Loading dump: path=${path}".green)
      DumpIO.props(path) match {
        case Left(t) =>
          val msg = s"invalid path=$path"
          log.error(cause=t, message=msg)
          // TODO unlock
          sender ! msg
          context.stop(self)
        case Right(a) =>
          val dumpIO = context.actorOf(a)
          dumpIO ! DumpIO.ReadNext
          context.become(load(dumpIO, sender)())
      }
    case DumpProcessor.Save(path) =>
      log.info(s"Saving dump: path=${path}".green)
      val timestamp = LocalDateTime.now.nn.format(DateTimeFormatter.ofPattern("yyyy.MM.dd-HH.mm.ss"))
      val dumpPath = s"${path}/rks_dump_${timestamp}"
      DumpIO.props(dumpPath) match {
        case Left(t) =>
          val msg = s"invalid path=$path"
          log.error(cause=t, message=msg)
          // TODO unlock
          sender ! msg
          context.stop(self)
        case Right(a) =>
          val dumpIO = context.actorOf(a)
          self ! DumpIO.PutDone
          val it = db.newIterator().nn
          it.seekToFirst()
          context.become(save(it, dumpIO, sender, dumpPath)())
      }
  }

  def save(it: RocksIterator, dumpIO: ActorRef, client: ActorRef, dumpPath: String): () => Receive = {
    () => {
      case _: DumpIO.PutDone.type =>
        if it.isValid() then
          dumpIO ! DumpIO.Put(Vector(it.key().nn -> it.value().nn))
          it.next()
        else
          log.info(s"Dump is saved: path=${dumpPath}".green)
          // TODO unlock
          client ! dumpPath
          dumpIO ! PoisonPill
          context.stop(self)
    }
  }

  def load(dumpIO: ActorRef, client: ActorRef): () => Receive = {
    var keysNumber: Long = 0L
    var size: Long = 0L
    var ksize: Long = 0L

    () => {
      case res: DumpIO.ReadNextRes =>
        dumpIO ! DumpIO.ReadNext
        keysNumber = keysNumber + res.kv.size
        res.kv.foreach{ d =>
          ksize = ksize + d._1.size
          size = size + d._2.size

          db.put(d._1, d._2)
        }

      case _: DumpIO.ReadNextLast.type =>
        log.info(s"load info: load is completed, total keys=${keysNumber}, size=${size}, ksize=${ksize}")
        log.info("Dump is loaded".green)
        client ! "done"
        // TODO unlock
        dumpIO ! PoisonPill
        context.stop(self)
    }
  }
}

extension (value: String)
    def green: String = s"\u001B[32m${value}\u001B[0m"