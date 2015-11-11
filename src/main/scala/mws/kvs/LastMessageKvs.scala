package mws.kvs

class LastMessageKvs(kvs: Kvs, list: String) extends Kvs.Wrapper(kvs, list) with Kvs.Iterable
