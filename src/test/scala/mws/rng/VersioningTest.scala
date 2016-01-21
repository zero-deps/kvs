package mws.rng

import akka.cluster.VectorClock
import org.scalatest.{Matchers, FlatSpec}

object VersioningTest {
  val old = new VectorClock()
  val conflict = new VectorClock().:+("n0")

  val eq_0 = new VectorClock().:+("n1")
  val eq_1 = new VectorClock().:+("n1")
  val eq_2 = new VectorClock().:+("n1")

  val newest = new VectorClock().:+("n1").:+("n1")
}

class VersioningTest extends FlatSpec with Matchers {
  import VersioningTest._

  "An order fun" should "return last and no one to update" in {
    val rez = order(List((eq_0, 2L), (eq_1, 2L), (eq_2, 2L)), identity[(VectorClock, Long)])
    rez._1._1 should be(eq_0)
    rez._2 should be(Nil)
  }

  it should "return last and old to update" in {
    val rez = order(List((eq_0, 2L), (eq_1, 2L), (old, 0L), (eq_2, 2L)), identity[(VectorClock, Long)])
    rez._1._1 should be(eq_0)
    rez._2 should be(List((old, 0L)))
  }

  it should "return last and update old conflicted" in {
    val rez = order(List((eq_0, 2L), (eq_1, 2L), (conflict, 1L), (eq_2, 2L)), identity[(VectorClock, Long)])
    rez._1._1 should be(eq_0)
    rez._2 should be(List((conflict, 1L)))
  }

  it should "provide last-write-win if conflict present" in {
    val rez = order(List((eq_0, 2L), (eq_1, 2L), (conflict, 5L), (eq_2, 2L)), identity[(VectorClock, Long)])
    rez._1._1 should be(conflict)
    rez._2 should be(List((eq_0, 2L), (eq_1, 2L), (eq_2, 2L)))
  }

  it should "return newest and outdated for update" in {
    val rez = order(List((eq_0, 2L), (newest, 3L), (eq_1, 2L), (eq_2, 2L)), identity[(VectorClock, Long)])
    rez._1._1 should be(newest)
    rez._2 should be(List((eq_0, 2L), (eq_1, 2L), (eq_2, 2L)))
  }
}
