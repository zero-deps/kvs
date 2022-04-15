# Abstract scala type database

[![CI](https://img.shields.io/github/workflow/status/zero-deps/kvs/ci)](https://github.com/zero-deps/kvs/actions/workflows/test.yml)
[![MIT](https://img.shields.io/github/license/zero-deps/kvs)](LICENSE)
[![Documentation](https://img.shields.io/badge/documentation-pdf-yellow)](kvs.pdf)
[![Paper](https://img.shields.io/badge/paper-pdf-lightgrey)](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
[![LoC](https://img.shields.io/tokei/lines/github/zero-deps/kvs)](#)

Abstract Scala storage framework with high-level API for handling linked lists of polymorphic data (feeds).

KVS is highly available distributed (AP) strong eventual consistent (SEC) and sequentially consistent (via cluster sharding) storage. It is used for data from sport and games events. In some configurations used as distributed network file system. Also can be a generic storage for application.

Designed with various backends in mind and to work in pure JVM environment. Implementation based on top of KAI (implementation of Amazon DynamoDB in Erlang) port with modification to use akka-cluster infrastructure.

Currently main backend is RocksDB to support embedded setup alongside application. Feed API (add/entries/remove) is built on top of Key-Value API (put/get/delete).

## Usage

Add project as a git submodule.

## Structure

* `./feed` -- Feed over Ring
* `./search` -- Seach over Ring
* `./ring` -- Ring on Akka Cluster
* `./sharding` -- Cluster Sharding on ZIO
* `./src` -- Example apps and tests

## Test & Demo

```bash
sbt test
sbt run
```
