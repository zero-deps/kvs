package kvs.rng

import akka.actor.*

case class Watch(a: ActorRef)
case class Select(node: Node, path: String)

object SelectionMemorize extends ExtensionId[SelectionMemorize] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): SelectionMemorize =
    new SelectionMemorize(system)

  override def lookup: SelectionMemorize.type = SelectionMemorize
}

trait ActorRefStorage {
  def get(node: Node, path: String): Either[ActorRef, ActorSelection]
  def put(n: (Node, String), actor: ActorRef): Unit
  def remove(node: (Node, String)): Unit
}

class SelectionMemorize(val s: ActorSystem)  extends  Extension with ActorRefStorage {

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
      context.watch(ref)
      storage.put((n.asInstanceOf[Node], ref.path.name), ref)
    case Terminated(actor) => 
      storage.remove((addr(actor), actor.path.name))
    case _ =>
  }
}
