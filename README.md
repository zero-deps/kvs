# Abstract scala type database

![ci](https://github.com/zero-deps/kvs/workflows/ci/badge.svg)

Abstract Scala storage framework with high-level API for handling linked lists of polymorphic data (feeds).

KVS is highly available distributed (AP) strong eventual consistent (SEC) and sequentially consistent (via cluster sharding) storage. It is used for data from sport and games events. In some configurations used as distributed network file system. Also can be a generic storage for application.

Designed with various backends in mind and to work in pure JVM environment. Implementation based on top of KAI (implementation of Amazon DynamoDB in Erlang) port with modification to use akka-cluster infrastructure.

Currently main backend is RocksDB to support embedded setup alongside application. Feed API (add/entries/remove) is built on top of Key-Value API (put/get/delete).

## Usage

Add project as a git module.

## Backend

* Ring
* RocksDB
* Memory
* FS
* SQL
* etc.

## Test

```bash
sbt> test
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
