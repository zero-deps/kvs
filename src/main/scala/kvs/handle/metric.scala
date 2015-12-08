package mws.kvs
package handle

import store._

/**
 * Metric type handler.
 *
 * Handle Metric for stats application.
 * Fully based on Message handler.
 */
object MetricHandler {

  val f  = (a:Metric) => Message(name="message",key=a.key, data=a.data)
  val g  = (b:Message)=> Metric( name="metric", key=b.key, data=b.data)
  val h = (k:String) => s"message${k.stripPrefix("metric")}"

  implicit val mth = Handler.by[Metric,Message](f)(g)(h)
}
