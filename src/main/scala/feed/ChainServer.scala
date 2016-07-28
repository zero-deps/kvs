package feed

import java.nio.ByteBuffer
import akka.actor.{ActorLogging, ActorRef, FSM}
import akka.serialization.SerializationExtension
import mws.rng._
import org.iq80.leveldb.{DB, WriteBatch}
import scala.annotation.tailrec

trait ChainData
case class ChainInfo(prev: ActorRef, next: ActorRef) extends ChainData
case class HeadChain(next: ActorRef) extends ChainData
case class TailChain(prev: ActorRef) extends ChainData
case object Blank extends ChainData

trait Role
case object Head extends Role
case object Tail extends Role
case object Regular extends Role

object ChainServer {
  val veryHeadIndex = "hd"
  val veryTailIndex = "tl"

  //TODO send byte_arrays instead of objects
  case class AddReplica(meta: (String, EntryMeta), prev: Option[(String, Entry)], v: (String, Entry))
}

/*
 * TODO :
 * - weak consistency -> read from local node instead of tail.
 */
class ChainServer(s: DB) extends FSM[Role, ChainData] with ActorLogging {
  import ChainServer._

  startWith(Regular, Blank)

  val serialization = SerializationExtension(context.system)

  def bytes(any: Any): Array[Byte] = any match {
    case b: Bucket => ByteBuffer.allocate(4).putInt(b).array()
    case anyRef: AnyRef => serialization.serialize(anyRef).get
  }

  def fromBytesList[T](arr: Array[Byte], clazz: Class[T]): Option[T] = Option(arr) match {
    case Some(a) => Some(serialization.deserialize(a, clazz).get)
    case None => None
  }

  def withBatch[R](body: WriteBatch ⇒ R): R = {
    val batch = s.createWriteBatch()
    try {
      val r = body(batch)
      s.write(batch)
      r

    } finally {
      batch.close()
    }
  }

  when(Head) {
    case Event(Add(fid, v), HeadChain(next)) =>
      //updated feed info + last tail id
      val meta: Option[EntryMeta] = fromBytesList(s.get(bytes(s"$fid:info")), classOf[EntryMeta])
      val updMeta: EntryMeta = meta match {
        case None => (id(fid, 0), id(fid, 0), 0)
        case Some(m) => m.copy(_2 = id(fid, m._3 + 1), _3 = m._3 + 1)
      }
      val prevEntry: Option[(String, Entry)] = meta.flatMap(m => fromBytesList(bytes(m._2), classOf[Entry])) match {
        case None => None
        case Some(e) => Some((meta.get._2, e))
      }
      val prevInx = meta match {
        case None => veryHeadIndex
        case Some(m) => m._2
      }
      val entry = (prevInx, v, veryTailIndex)

      withBatch(batch => {
        //upd prev entry if present
        prevEntry.map(p => batch.put(bytes(p._1), bytes(p._2)))
        //meta upd with new tail and id_counter
        batch.put(bytes(s"$fid:info"), bytes(updMeta))
        // add
        batch.put(bytes(updMeta._2), bytes(entry))
      })
      next ! AddReplica((s"$fid:info", updMeta), prevEntry, (updMeta._2, entry))
      stay()
  }

  when(Regular) {
    case Event(msg@AddReplica(meta, prev, value), ChainInfo(prevChain, next)) =>
      withBatch(batch => {
        batch.put(bytes(meta._1), bytes(meta._2))
        prev.map(p => batch.put(bytes(p._1), bytes(p._2)))
        batch.put(bytes(value._1), bytes(value._2))
      })
      next ! msg
      stay()
  }

  when(Tail) {
    case Event(msg@AddReplica(meta, prev, value), TailChain(prevChain)) =>
      withBatch(batch => {
        batch.put(bytes(meta._1), bytes(meta._2))
        prev.map(p => batch.put(bytes(p._1), bytes(p._2)))
        batch.put(bytes(value._1), bytes(value._2))
      })
      log.info(s"[feed]tail_add $msg")
      // TODO notify client(synch)
      stay()

    case Event(t@Traverse(fid, start, count), TailChain(prev)) =>
      val client = sender()
      val startIndx = start match {
        case None => fromBytesList(s.get(bytes(s"$fid:info")), classOf[EntryMeta]).fold("tail")(_._1)
        case Some(id) => id
      }
      val rez: List[Value] = iterDB(startIndx, count, List.empty[Value])
      log.info(s"[feed]$t -> $rez")
      client ! rez
      stay()
  }

  @tailrec
  private def iterDB(id: String, count: Option[Int], acc: List[Value]): List[Value] = (id, count) match {
    case (`veryTailIndex`, _) => acc
    case (_, Some(0)) => acc
    case (someId, Some(countIttr)) => fromBytesList(s.get(bytes(someId)), classOf[Entry]) match {
      case None => acc
      case Some(e) => iterDB(e._3, Some(countIttr - 1), e._2 :: acc)
    }
  }

  whenUnhandled {
    case Event(m, _) =>
      log.info(s"$m ignoring") // TODO REMOVE, only for debuging
      stay()
  }
}
