package mws.kvs

class LastMetricKvs(kvs: Kvs, list: String) extends Kvs.Wrapper(kvs, list) with Kvs.Iterable
