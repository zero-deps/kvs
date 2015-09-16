package mws.rng

import akka.actor._

case class Select(node: Node)

trait ActorStorage {
  def get(node: Node): Either[ActorRef, ActorSelection]
  def put(n: Node, actor: ActorRef)
  def remove(node: Node): Unit
}

case class Watch(a: ActorRef)

class Monitor(storage: ActorStorage) extends Actor {

  override def receive: Actor.Receive = {
    
    case Select(node) =>
      val path = RootActorPath(node) / "user" / "ring_store"
      context.system.actorSelection(path) ! Identify(node)
    case ActorIdentity(n, Some(ref)) if n.isInstanceOf[Node] =>
      storage.put(n.asInstanceOf[Node], ref)
    case Watch(actor) => context.watch(actor)
    case Terminated(actor) => storage.remove(actor.path.address)
    case _ =>
  }
  
}

class SelectionMemorize(val s: ActorSystem) extends ActorStorage {

  @volatile
  private var map = Map.empty[Node, ActorRef]
  private val monitor = s.actorOf(Props(classOf[Monitor], this))

  override def get(n: Node): Either[ActorRef, ActorSelection] = {
    map.get(n) match {
      case Some(actor) => Left(actor)
      case None => monitor ! Select(n)
        val path = RootActorPath(n) / "user" / "ring_store"
        Right(s.actorSelection(path))
    }
  }

  override def put(n: Node, a : ActorRef) = map = map + (n ->a )

  override def remove(n: Node) = map = map - n
}

