# Abstract scala type database

![test](https://github.com/zero-deps/kvs/workflows/test/badge.svg)

## Version

`main`: current version; can be added as git-submodule  
`4.x`: legacy but proven version; published to jcenter

## Backend

* Ring
* Memory
* FS
* SQL
* LevelDB
* etc.

## Test

```bash
sbt test
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
