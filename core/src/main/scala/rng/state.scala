package zd.rng

sealed trait FsmState
final case object ReadyCollect extends FsmState
final case object Collecting extends FsmState
final case object Sent extends FsmState
