# Ring datastore #

Scala implementation of Kai (originally implemented in erlang).
Kai is a distributed key-value datastore, which is mainly inspired
by Amazon's Dynamo.

## Overview ##

Ring is a distributed key-value data storage implemented on top of akka and injected as akka extension.
To reach fault tolerance and scalability ring resolve next problems:

| Problems               | Technique
| :----------------------: | :------------------------------------------------------------------------------------
| membership and failure detection | reused akka's membership events that uses gossip for communication. FD also reused from akka.
| data partitioning | consistent hashing
| high availability to wright | vector clocks
| handling nodes failures | quorum
| recovering from permanent node failure | fix on read

   
### Consistent hashing ###

To figure out where the data for a particular key goes in that cluster you need to apply a hash function to the key.
Just like a hashtable, a unique key maps to a value and of course the same key will always return the same hash code.
In very first and simple version of this algorithm the node for particular key is determined by hash(key) mod n, where n is a number of
nodes in cluster. This works well and trivial in implementation but when new node join or removed from cluster we got a problem,
every object is hashed to a new location.
The idea of the consistent hashing algorithm is to hash both node and key using the same hash function.
As result we can map the node to an interval, which will contain a number of key hashes. If the node is removed
then its interval is taken over by a node with an adjacent interval.
  
### Vector clocks ###

Vector clocks is an algorithm for generating a partial ordering of events in a distributed system and detecting causality violations. (from wikipedia.org)
Vector clocks help us to determine order in which data writes was occurred. This provide ability to write data from one node and after that 
merge version of data. Vector clock is a list of pairs of node name and number of changes from this node.
When data writs first time the vector clock will have one entity ( node-A : 1). Each time data amended the counter is incremented.

### Quorum ###

Quorum determines the number of nodes that should be participated in operation.  Quorum-like system configured by values: R ,W and N. R is the
minimum number of nodes that must participate in a successful read operation. W is the minimum number of nodes that must participate.
N is a preference list, the max number of nodes that can be participated in operation. Also quorum can configure balance of latency
for read and write operation.
 In order to keep data strongly consistent configuration should obey rules:
 1) R + W > N
 2) W > V/2

### Mixing in Ring ###


  
1. Extensions starts on node with configuration:
  buckets - size of "hash map"
  virtual nodes - number of nodes each node is responsible to store
  quorum

2. After node join cluster a membership round occurs.
  i) hash from (node.address + i ). Updating SortedMap of hashes that points to node. As result we had sequence of sorted integers [0 - Int.MAX] pointing to node.

  ii) bucket range is Int.Max / 1024.
  for each bucket -> find corresponding nodes. get nearest value + try vNodes to the right other nodes. Stop if find N nodes.

  update map of bucket -> preference list, if needed. Pref. list taken from above operation.

  bucket with updated preference list is updates and self in this list. Either delete data for bucket or get from other nodes.

3. API requests can be handled. put get delete
  e.g. GET(key). hash from key. [0 - INt.MAX] / 1024. this is bucket.
  we already know nodes that responsive for any bucket. Spawn actor to ask and merge result if needed.

[Consistent hashing - that`s how data is saved](https://en.wikipedia.org/wiki/Consistent_hashing)

[What is quorum]((https://en.wikipedia.org/wiki/Quorum_(distributed_computing))



## Configuration ##

To configure rng application on your cluster the next configs are available. Default values specified below.
### *!!!*  _`quorum` and `leveldb.dir` must be configured for each envronment_ *!!!*

### *!!!*  And if default value is suitable for particular deployment, rewrite is not needed. *!!!*

```
ring {
  quorum=[1,1,1]  #N,W,R.
  buckets=1024
  virtual-nodes=128
  hashLength=32
  gather-timeout = 3
  ring-node-name="ring_node"
  leveldb {
    native = true
    dir = "fav-data"
    checksum = false
    fsync = false
  }
}
```

`quorum` template is [N,W,R]: N - number of nodes in bucket (in other words the number of copies). R - number of nodes that must  be participated in successful read operation.
W - number of nodes for successful write.

To keep data consistent the quorums have to obey the following rules:
1. R + W > N
2. W > N/2

Or use the next hint:
* single node cluster [1,1,1]
* two nodes cluster [2,2,1]
* 3 and more nodes cluster [3,2,2]

__NB!__ If quorum fails on write operation, data will not be saved. For example if 2 nodes and [2,2,1] after 1 node down
  the cluster becomes readable but not writeable.
 
| name               | description 
| :----------------: | :------------------------------------------------------------------------------------
| `buckets`          | Number of buckets for key. Think about this like the size of HashMap. At the default value is appropriate.
| `virtual-nodes`    | Number of virtual nodes for each physical.
| `hashLength`       | Lengths of hash from key
| `gather-timeout`   | This configuration should be specified by client developer.<br>Number of seconds that requested cluster will wait for responses from nodes that persist data by given key.<br>Result of request will returned to client on either received data from other nodes(number specified in quorum) or after this time out.<br>The main cause if this function is to avoid TimeoutException in case some  nodes become Unreachable for some period or another situation when requested node is not able to gather info from other nodes.
| `leveldb.native`   | usage of native or java implementation if LevelDB
| `leveldb.dir`      |  directory location for levelDB storage. 
| `leveldb.checksum` |  checksum
| `leveldb.fsync`    |  if true levelDB will synchronise data to disk immediately.


## Usage ##

Ring is available as akka extension.

`val ring = HashRing(system);` - create HashRing extension on specified actor system. Akka cluster should be created before.


## Docker ##

### Install ###
Store the docker repo key

  > $ sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9

  > $ sudo sh -c "echo deb https://get.docker.com/ubuntu docker main > /etc/apt/sources.list.d/docker.list"
  > $ sudo apt-get update
  > $ sudo apt-get install lxc-docker

Verify installation with ubuntu image

  > $ sudo docker run -i -t ubuntu /bin/bash

### sbt-docker plugin ###

Run sbt task to create basic docker container

  > docker:publishLocal

### Run docker nodes ###

  > docker run -P -t -i --rm --name seed playtech/rng:1.0-78-g96322f3
  > docker run -P -t -i --rm --name c1 --link seed:seed playtech/rng:1.0-78-g96322f3
  > docker run -P -t -i --rm --name c2 --link seed:seed playtech/rng:1.0-78-g96322f3
  
| name    | description
| :-----: | :---------------------------------
| -i      | interactive mode. keep STDIN open
| -t      | allocate pseudo-TTY
| -P      | publish all exposed ports to host
| --rm    | remove after stop
| --link  | link to another container by `alias:name` scheme

### JMX ###

Execute `bin/akka-cluster node(ip) port(9998)` to check the cluster status.