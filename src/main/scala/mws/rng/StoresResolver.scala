package mws.rng

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

object Select

trait ActorStorage {
  def get(node: Node): Option[ActorRef]
  def remove(node: Node): Unit
}

class IdentifyActor(val n: Node) extends Actor with ActorLogging{
  private var client: ActorRef = _

  override def receive: Receive = {
    case m @ Select =>
      client = sender()
      lookup
    case ActorIdentity(`n`, Some(ref)) => 
      client ! Some(ref)
      context.stop(self)
    case ActorIdentity(`n`, None) => 
      client ! None
      context.stop(self)
    case _ => 
      client ! None
      context.stop(self)
  }

  def lookup = {
    val path = RootActorPath(n) / "user" / "ring_store"
    context.system.actorSelection(path) ! Identify(n)
  }
}

case class Watch(a: ActorRef)

class Monitor(storage: ActorStorage) extends Actor {

  override def receive: Actor.Receive = {
    case Watch(actor) => context.watch(actor)
    case Terminated(actor) => storage.remove(actor.path.address)
  }
}

class SelectionMemorize(val s: ActorSystem) extends ActorStorage {

  implicit val timeout = Timeout(3.second)
  @volatile
  private var map = Map.empty[Node, ActorRef]
  private val monitor = s.actorOf(Props(classOf[Monitor], this))

  override def get(n: Node): Option[ActorRef] = {
    map.get(n) match {
      case Some(actor) => Some(actor)
      case None =>
        val f = ask(s.actorOf(Props(classOf[IdentifyActor], n)), Select).mapTo[Option[ActorRef]]
        val ar = Await.result(f, timeout.duration)
        ar.map { actorR =>
          map = map.updated(n, actorR)
          monitor ! Watch(actorR)
        }
        ar
    }
  }

  override def remove(n: Node) = map = map - n
}

