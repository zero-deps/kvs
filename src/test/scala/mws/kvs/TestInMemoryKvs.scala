package mws.kvs

import akka.actor.Props

object TestInMemoryKvs {
  def props(): Props = Props[TestInMemoryKvs]
}

class TestInMemoryKvs extends TestKvs with InMemoryKvs
