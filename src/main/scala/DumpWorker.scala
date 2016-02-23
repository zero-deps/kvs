package mws.rng

import akka.cluster.VectorClock
import akka.actor.{FSM, Props}
import mws.rng.store.{BucketGet, GetBucketResp, WriteStore, BucketPut}
import java.util.Calendar
import scala.collection.SortedMap
import java.io.File
import org.iq80.leveldb._

case class DumpData(current: Bucket, prefList: PreferenceList, collected: List[Option[List[Data]]])

class DumpWorker(buckets: SortedMap[Bucket, PreferenceList]) extends FSM[FsmState, DumpData]{
    val leveldbFactory = org.iq80.leveldb.impl.Iq80DBFactory.factory
    val timestamp = Calendar.getInstance().getTime().toString.replaceAll(" ", "")
    val db = leveldbFactory.open(new File(s"rng_dump_$timestamp"), new Options().createIfMissing(true))
    val dumpStore = context.actorOf(Props(classOf[WriteStore], db))
    val stores = SelectionMemorize(context.system)
    val bucketsNum = context.system.settings.config.getInt("ring.buckets")
    startWith(ReadyCollect, DumpData(0, Nil, Nil))
    
    when(ReadyCollect){
        case Event(Dump, data ) => 
            data.current match {
                case `bucketsNum` => stop()
                case b =>  
                    buckets(b).foreach{n => stores.get(n, "ring_readonly_store").fold(_ ! BucketGet(b), _ ! BucketGet(b))}
                    goto(Collecting) using(DumpData(b, buckets(b), Nil))
            }
    }

    when(Collecting){
        case Event(GetBucketResp(b,data), state) => 
            remove(sender().path.address, state.prefList) match {
                case Nil => 
                    val bucketData = mergeBucketData(
                        (data :: state.collected).flatten.foldLeft(List.empty[Data])((acc, l) => l ::: acc), Nil)
                    dumpStore ! BucketPut(bucketData)
                    goto(ReadyCollect) using(DumpData(state.current + 1, Nil, Nil))
                case l => stay() using(DumpData(state.current ,l ,data :: state.collected))
            } 
    }

    def remove(n: Node, l: List[Node]): List[Node] = l match {
        case Nil => Nil
        case h :: t if h == n => t
        case h :: t => h :: remove(n,t) 
    }

    initialize()
}