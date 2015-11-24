package mws.kvs

class LastMessageKvs(kvs: StKvs, list: String) extends StKvs.Wrapper(kvs, list) with StKvs.Iterable
