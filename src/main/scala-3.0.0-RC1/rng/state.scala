package zd.rng

sealed trait FsmState
case object ReadyCollect extends FsmState
case object Collecting extends FsmState
case object Sent extends FsmState
