package mws.rng

import akka.actor._

case class Watch(a: ActorRef)
case class Select(node: Node, path: String)

trait ActorRefStorage {
  def get(node: Node, path: String): Either[ActorRef, ActorSelection] //TODO return only actorRef, wait first time
  def put(n: (Node, String), actor: ActorRef)
  def remove(node: (Node, String)): Unit
}

class SelectionMemorize(val s: ActorSystem)  extends ActorRefStorage {

  @volatile
  private var map = Map.empty[(Node, String), ActorRef]
  private val monitor = s.actorOf(Props(classOf[Monitor], this))

  override def get(n: Node, path: String): Either[ActorRef, ActorSelection] = {
    map.get((n, path)) match {
      case Some(actor) => Left(actor)
      case None => monitor ! Select(n, path)
        val fullPath = RootActorPath(n) / "user" / path
        Right(s.actorSelection(fullPath))
    }
  }

  override def put(n: (Node, String), a : ActorRef) = map = map + (n ->a )
  override def remove(n: (Node, String)) = map = map - n
}

class Monitor(storage: ActorRefStorage) extends Actor {

  override def receive = {
    case Select(node, path: String) =>
      val fullPath = RootActorPath(node) / "user" / path
      context.system.actorSelection(fullPath) ! Identify(node)
    case ActorIdentity(n, Some(ref)) if n.isInstanceOf[Node] =>
      storage.put((n.asInstanceOf[Node], ref.path.name), ref)
    case Watch(actor) => context.watch(actor)
    case Terminated(actor) => storage.remove((actor.path.address, actor.path.name))
    case _ =>
  }
}
