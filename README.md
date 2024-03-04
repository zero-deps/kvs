# Scala Abstract Type Database

[![Documentation](https://img.shields.io/badge/documentation-pdf-yellow)](kvs.pdf)
[![Paper](https://img.shields.io/badge/paper-pdf-lightgrey)](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)

This open-source project presents an abstract storage framework in Scala, offering a high-level API tailored for managing linked lists of polymorphic data, referred to as 'feeds.' The system, known as KVS (Key-Value Storage), boasts attributes such as high availability, distributed architecture (AP), strong eventual consistency (SEC), and sequential consistency achieved through cluster sharding. Its primary application involves handling data from sports and gaming events, but it can also serve as a distributed network file system or a versatile general-purpose storage solution for various applications.

The design philosophy behind KVS encompasses versatility, with support for multiple backend implementations and compatibility within a pure JVM environment. The implementation is grounded in the KAI framework (an Erlang-based Amazon DynamoDB implementation), adapted to utilize the pekko-cluster infrastructure.

At its core, KVS relies on RocksDB as the primary backend, enabling seamless integration in embedded setups alongside applications. The central Feed API, facilitating operations like addition, entry retrieval, and removal, is constructed upon the foundation of the Key-Value API, which includes functions for putting, getting, and deleting data.

## Usage

Add project as a git submodule.

## Project Structure

* `./feed`: Introduces the Feed over Ring concept
* `./search`: Offers Search over Ring functionality
* `./sort`: Implements a Sorted Set on Ring
* `./ring`: Establishes a Ring structure using Pekko Cluster
* `./sharding`: Addresses Sequential Consistency & Cluster Sharding aspects
* `./src`: Contains illustrative sample applications and comprehensive tests

## Test & Demo

```bash
sbt test
sbt run
```
