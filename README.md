# Abstract scala type database.

[![Scala CI](https://github.com/zero-deps/kvs/workflows/Scala%20CI/badge.svg?branch=master)](https://github.com/zero-deps/kvs/actions?query=branch%3Amaster)

[Bintray releases](https://bintray.com/zero-deps/maven/kvs-core#release)

Key Value Storage

[Example](https://github.com/zero-deps/kvs/blob/master/demo/src/main/scala/Run.scala)

More documentation check in `docs` directory.

More examples check in `src/test/scala` directory.

## Backend

 * Ring
 * Memory
 * FS
 * etc.

## Test

```bash
sbt test
sbt 'project demo' run
```

## Resources

### Chain Replication

[Chain Replication in Theory and in Practice](http://www.snookles.com/scott/publications/erlang2010-slf.pdf)

[Chain Replication for Supporting High Throughput and Availability](http://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf)

[High-throughput chain replication for read-mostly workload](https://www.cs.princeton.edu/courses/archive/fall15/cos518/papers/craq.pdf)

[Leveraging Sharding in the Design of Scalable Replication Protocols](https://ymsir.com/papers/sharding-socc.pdf)

[Byzantine Chain Replication](http://www.cs.cornell.edu/home/rvr/newpapers/opodis2012.pdf)

### Consensus Algorithm

[RAFT](https://raft.github.io/raft.pdf)
[SWIM](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
