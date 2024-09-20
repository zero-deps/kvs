# KVS (Key-Value Storage)

## Scala Abstract Type Database

![Production Ready](https://img.shields.io/badge/Project%20Stage-Production%20Ready%20(main)-brightgreen.svg)
![Development](https://img.shields.io/badge/Project%20Stage-Development%20(next)-yellowgreen.svg)

This open-source project presents an abstract storage framework in Scala, offering a high-level API tailored for managing linked lists of polymorphic data, referred to as 'feeds.' The system, known as KVS (Key-Value Storage), boasts attributes such as high availability, distributed architecture (AP), strong eventual consistency (SEC), and sequential consistency achieved through cluster sharding. Its primary application involves handling data from sports and gaming events, but it can also serve as a distributed network file system or a versatile general-purpose storage solution for various applications.

The design philosophy behind KVS encompasses versatility, with support for multiple backend implementations and compatibility within a pure JVM environment. The implementation is grounded in the KAI framework (an Erlang-based Amazon DynamoDB implementation), adapted to utilize the pekko-cluster infrastructure.

At its core, KVS relies on RocksDB as the primary backend, enabling seamless integration in embedded setups alongside applications. The central Feed API, facilitating operations like addition, entry retrieval, and removal, is constructed upon the foundation of the Key-Value API, which includes functions for putting, getting, and deleting data.

## Usage

Add the project as a git submodule or publish in repository of own choice.

## Components

* `rng`: Establishes a Ring structure using Pekko Cluster
* `search`: Offers Search over KVS functionality
* `consistency`: Sequential consistency for complex inserts (e.g. feeds)
* `feed`: Introduces the Feed over Ring concept
* `sharding`: Pekko's Cluster Sharding abstraction for sequential consistency
* `sort`: Implements a Sorted Set on Ring

## Test and Demo

```sh
sbt test
sbt run
```

# Documentation

## Abstract

KVS is an abstract **Scala Types** database that enables the construction of storage schemes centered around a **linked list of entities** (data feeds). It is supported by multiple backend storage engines, making it suitable for various needs. When used with the **RING backend**, it becomes a powerful tool for managing distributed data while ensuring **sequential consistency** when integrated with **FeedServer**.

## Core Components

### Services Handlers

- **KVS**: An abstract data types **Key-Value Storage** for Scala value types, featuring a simple `put/get` API, as well as extended operations for managing data feeds.
- **Backend Storage Engines**:
  - **RING**: A distributed, scalable, and fault-tolerant key-value store.
  - **LevelDB**: For non-clustered, single-node environments.
  - **Memory**: In-memory storage, useful for caching or testing.
  - **Filesystem**: Uses the filesystem for storage.

### Services Handler Specificity

Each service can operate on specific data types, requiring logic for serializing, pickling, or marshalling the data. Handlers ensure compatibility with particular data types during the serialization process.

## Datastore JMX Interface

The **Java Management Extensions (JMX)** interface allows for remote management of the KVS system. KVS registers two **MBeans**: `Kvs` and `Ring`.

- **MBean Access**: Tools like `jconsole` can be used for interacting with these resources, typically located in `$JDKHOME/bin`.
- **Core JMX Operations**:
  - **allStr(fid: String): String**: Returns a string representation of all entities within a specified feed.
  - **Kvs:save**: Initiates a save operation to create a zip archive of distributed data across the nodes.
  - **Kvs:load(path)**: Loads data from a previously saved archive, ensuring quorum configuration is satisfied during the write process.
  - **Ring:get(key: String): String**: Retrieves a value by its key.
  - **Ring:put(key: String, data: String): String**: Inserts a value associated with a key (primarily for testing purposes).
  - **Ring:delete(key: String)**: Deletes the value associated with a key.

> **Note**: RNG becomes readonly during the save and load processes to maintain consistency.

## Features and Capabilities

- **Sequential Consistency**: Managed through the **FeedServer**, KVS ensures sequential consistency in operations.
- **Linked Lists**: Data entries in KVS are stored as a doubly-linked list, enabling easy navigation and manipulation of entities.
- **Scala Pickling**: Serialization is managed using **Scala Pickling**, which requires defining picklers at compile-time for your data.
- **Backend Flexibility**: The system supports multiple backends (LevelDB, RING, Memory), allowing it to be tailored for different use cases.
- **Scalaz Tagged Types**: Leverages **Scalaz** tagged types for enhanced type safety at compile-time, ensuring operations on incompatible data types are caught early.

## Distributed Data Handling

### RING Datastore

**RING** (or RNG) is a distributed **key-value store** inspired by Amazon’s **Dynamo**. It is implemented using **Akka** for high availability, fault tolerance, and scalability.

#### Configuration Options

- **Quorum Configuration**: Defined by the parameters `N`, `W`, and `R`:
  - **N**: Total number of replicas.
  - **R**: Number of replicas required for a successful read.
  - **W**: Number of replicas required for a successful write.
  
  **Rule for Consistency**: `R + W > N`

#### Consistent Hashing

RING uses **consistent hashing** to distribute data across the cluster, ensuring minimal data movement when nodes join or leave.

#### Vector Clocks

**Vector clocks** are employed to maintain a partial ordering of events across the cluster, detecting conflicts during write operations and resolving them during reads.

#### Fault Tolerance and Availability

**Gossip protocols** are utilized for membership and failure detection, ensuring fault tolerance and resilience across the cluster.

#### Default Configuration

- **Quorum**: `[1, 1, 1]`
- **Buckets**: `1024`
- **Virtual Nodes**: `128`
- **Hash Length**: `32`
- **Gather Timeout**: `3 seconds`

> This configuration ensures that the system works out-of-the-box for a single-node deployment.

## Metrics and Monitoring

KVS/RING exposes various metrics to track system health and load:

- **Disk/Memory Usage**: Tracks available disk space, file descriptors, swap usage, and IO wait times.
- **Read/Write Operations**: Monitors consistent reads and writes coordinated by each node.
- **Network Throughput**: Tracks latency and general network health.
- **Search Metrics**: Provides insights into indexing errors and search query performance.

These metrics can be monitored through the **JMX interface**, providing detailed insights into the system's performance.

## Future Improvements

To further enhance KVS, consider implementing the following:

1. **Secondary Indexes**: Support for secondary indexing to improve the performance of complex queries.
2. **Schema Versioning**: Implementing schema versioning would enable backward compatibility during upgrades.
3. **Better Error Handling**: Provide more detailed feedback during quorum failures and node crashes.
4. **Improved Serialization**: Explore alternatives to Scala Pickling for more efficient serialization, especially in cross-version compatibility scenarios.

## Conclusion

KVS is a flexible, distributed, and fault-tolerant key-value storage system designed for high scalability and sequential consistency. Its modular architecture and support for various backends make it a versatile choice for different data storage requirements.

## Resources

[Dynamo: Amazon's Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
