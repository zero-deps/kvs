package mws.rng

import akka.cluster.VectorClock
import akka.actor.{FSM, Props, ActorLogging}
import mws.rng.store.{BucketGet, GetBucketResp, WriteStore, BucketPut}
import java.util.Calendar
import scala.collection.SortedMap
import java.io.File
import org.iq80.leveldb._

case class DumpData(current: Bucket, prefList: PreferenceList, collected: List[Option[List[Data]]])

class DumpWorker(buckets: SortedMap[Bucket, PreferenceList], local: Node) extends FSM[FsmState, DumpData] with ActorLogging{
    val leveldbFactory = org.iq80.leveldb.impl.Iq80DBFactory.factory
    val timestamp = Calendar.getInstance().getTime().toString.replaceAll(" ", "")
    val filePath = s"rng_dump_$timestamp"
    val db = leveldbFactory.open(new File(filePath), new Options().createIfMissing(true))
    val dumpStore = context.actorOf(Props(classOf[WriteStore], db))
    val stores = SelectionMemorize(context.system)
    val lastBucket = context.system.settings.config.getInt("ring.buckets")
    startWith(ReadyCollect, DumpData(0, Nil, Nil))
    
    when(ReadyCollect){
        case Event(Dump, state ) => 
            buckets(state.current).foreach{n => stores.get(n, "ring_readonly_store").fold(_ ! BucketGet(state.current), _ ! BucketGet(state.current))}
            goto(Collecting) using(DumpData(state.current, buckets(state.current), Nil))
    }

    when(Collecting){ 
        case Event(GetBucketResp(b,data), state) => // TODO add timeout if any node is not responding.
            log.info(s"receive b=$b, data = ${data}")
            remove(if(sender().path.address.hasLocalScope) local else sender().path.address, state.prefList) match {
                case Nil => 
                    val bucketData = mergeBucketData(
                        (data :: state.collected).flatten.foldLeft(List.empty[Data])((acc, l) => l ::: acc), Nil)
                    if(bucketData.nonEmpty) {
                        log.info(s"bucket put $bucketData")
                        dumpStore ! BucketPut(bucketData)}
                    b+1 match {
                        case `lastBucket` => 
                            stores.get(self.path.address, "ring_hash").fold(_ ! DumpComplete(filePath), _ ! DumpComplete(filePath))
                            log.info("Dump complete, sending path to hash")
                            db.close
                            stop()
                        case nextBucket => 
                            buckets(nextBucket).foreach{n => stores.get(n, "ring_readonly_store").fold(_ ! BucketGet(nextBucket), _ ! BucketGet(nextBucket))}
                            stay() using(DumpData(nextBucket, buckets(nextBucket), Nil))
                    }
                case l => 
                    stay() using(DumpData(state.current ,l , data :: state.collected))
            } 
    }

    def remove(n: Node, l: List[Node]): List[Node] = l match {
        case Nil => Nil
        case h :: t if h == n => t
        case h :: t => h :: remove(n,t) 
    }

    initialize()
}