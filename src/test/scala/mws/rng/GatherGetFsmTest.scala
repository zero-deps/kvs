package mws.rng

import akka.testkit.TestFSMRef

class GatherGetFsmTest {

  
  val fsm = TestFSMRef(new GatherGetFsm(null, 3))
}
