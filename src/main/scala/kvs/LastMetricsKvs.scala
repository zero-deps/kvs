package mws.kvs

class LastMetricKvs(kvs: StKvs, list: String) extends StKvs.Wrapper(kvs, list) with StKvs.Iterable
