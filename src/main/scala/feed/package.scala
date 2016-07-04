import akka.actor.ActorRef
import mws.rng.Value

package object feed{
  type FID = String
	type Err = String
	type Resp = Either[Value, Err]

  type Entry = (String, Value, String) // prevId:String, v: Value, nextId: String
  type EntryMeta = (String, String, Long) // head, tail, ID_counter
  type Chain = Seq[ActorRef]

  def id(fid: String,i:Long) = s"$fid:$i"

  //API
  case class Add(fid: FID, v: Value)
  case class Traverse(fid: String, start: Option[String], count: Option[Int])
  case class Remove(fid: String, id: String)

}