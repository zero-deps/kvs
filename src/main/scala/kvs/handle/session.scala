package mws.kvs
package handle

import store._

/**
 * socket.io session persistence handler.
 */
object SessionHandler {
  implicit val ssh = Handler.by[Session,En[String]](identity)(identity)(identity)
}
