Ring datastore
===

Scala implmentation of Kai (originally implemented in erlang).
Kai is a distributed key-value datastore, which is mainly inspired
by Amazon's Dynamo.

## Overview


## Configuration

To configure rng application on your cluster the next configs are available. Default values specified below.
  

```
ring {
  quorum=[1,1,1]  #N,W,R.
  buckets=1024
  virtual-nodes=128
  hashLength=32
  gather-timeout = 5
  ring-node-name="ring_node"
  leveldb {
    native = true
    dir = "fav-data"
    checksum = false
    fsync = false
  }
}
```


* `quorum` template [N,W,R]: N - number of nodes in bucket (in other words the number of copies). R - number of nodes that must  be participated in successful read operation.
W - number of nodes for successful write.
To keep data consistent the quorums have to obey the following rules:
1. R + W > N
2. W > N/2
   
Or use the next hint:
* single node cluster [1,1,1]
* two nodes cluster [2,2,1]
* 3 and more nodes cluster [3,2,2]

__NB!__ if quorum fails on write operation, data will not be saved. So in case if 2 nodes and [2,2,1] after 1 node down
  the cluster becomes not writeable and readable.






## Usage

Ring is available as akka extension.

`val ring = HashRing(system);`

`ring.get("key")`

`ring.put("key", "val")`

ring.delete("key")`



## Docker

### Install
Store the docker repo key

  > $ sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9

  > $ sudo sh -c "echo deb https://get.docker.com/ubuntu docker main > /etc/apt/sources.list.d/docker.list"
  > $ sudo apt-get update
  > $ sudo apt-get install lxc-docker

Verify installation with ubuntu image

  > $ sudo docker run -i -t ubuntu /bin/bash

### sbt-docker plugin

Run sbt task to create basic docker container

  > docker:publishLocal

### Run docker nodes

  > docker run -P -t -i --rm --name seed playtech/rng:1.0-22-gdd6c507
  > docker run -P -t -i --rm --name c1 --link seed:seed playtech/rng:1.0-22-gdd6c507
  > docker run -P -t -i --rm --name c2 --link seed:seed playtech/rng:1.0-22-gdd6c507
  
| name    | description
| :-----: | :---------------------------------
| -i      | interactive mode. keep STDIN open
| -t      | allocate pseudo-TTY
| -P      | publish all exposed ports to host
| --rm    | remove after stop
| --link  | link to another container by `alias:name` scheme

### JMX

Execute `bin/akka-cluster node(ip) port(9998)` to check the cluster status.