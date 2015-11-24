package mws.kvs

package object favorite {

  type Attr = Map[String,String]
//  type Game = Tuple2[Attr,Int]
  type RecentGame = Tuple2[Attr,Int]
  type FavoriteGame = Tuple2[Attr,Int]

  trait Game {
    type G <: Tuple2[Attr,Int]
    //def id(g:G):String = g._1.getOrElse("id","")
//    def toString(_1)
  }

  object Game {
    type G = RecentGame

    def apply(attr:Attr, priority:Int) = new G(attr,priority)
    def unapply(g:G):Option[G] = Some(g)
    def id(g:G):String = g._1.getOrElse("id","")
    def withId(id:String)(implicit g:G):G = g.copy(_1 = g._1+("id"->id))
    def withPriority(p:Int)(implicit g:G):G = g.copy(_2=p)
  }

/*  class Game(attributes: Map[String, String], priority: Int = 0) extends Serializable {
    val id: String = attributes.getOrElse("id", "")

    def setId(_id: String): Game = if (_id == id) this else Game(attributes + ("id" -> _id), priority)

    def setPriority(_priority: Int): Game = Game(attributes, _priority)

    def getAttribute(name: String): String = attributes.getOrElse(name, "")

    override def equals(that: Any): Boolean = that match {
      case that: Game => that.attributes.filterNot(_._1 == "id") == attributes.filterNot(_._1 == "id")
      case _ => false
    }

    override def hashCode: Int = attributes.filterNot(_._1 == idName).hashCode()
    override def toString: String = (attributes + ("priority" -> priority.toString)).toString()
  }
*/

  import java.io.{Serializable => JSerializable}

  class Context(user: AnyRef, umsSecretKey: String, remoteAddress: Option[String],
               properties: Map[String, JSerializable], correlationId: Option[String],
               sessionId: Long) extends Serializable

//  type G <: Game
//  type FavoriteGames[G] <: List[G]

//  type C <: Context

}