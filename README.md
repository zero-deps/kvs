Ring datastore
===

Scala implmentation of Kai (originally implemented in erlang).
Kai is a distributed key-value datastore, which is mainly inspired
by Amazon's Dynamo.


## Usage

Ring is available as akka extension.
 
`val ring = HashRing(system);`

`ring.get("key")`

`ring.put("key", "val")`


### Configuration
TO configure rng application on your cluster add the following configuration.
Quorum template [N,R,W]: N - number of nodes in bucket. R - number of nodes that must  be participated in successful read operation.
W - number of nodes for successful write.

```
ring {
  quorum=[3,2,2]  #N,R,W. Change to [1,1,1] for single node
  buckets=1024
  virtual-nodes=128
  sync_interval=1000 # not implemented yet. Ignoring.
  hashLength=32
  ring-node-name="ring_node"
  leveldb {
    native = true
    dir = "fav-data"
    checksum = false
    fsync = false
  }
}
```

### Configuration

To join Ring node should have role that specified in rng configuration as`ring-node-name`


### Known issues

  - Synchronisation among buckets ( portions of virtual nodes in count or N from quorum). ATM is disabled.
  
  - read/write strategy if quorum not satisfied.

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

  > docker run -P -t -i --rm --name seed playtech/rng:1.0-SNAPSHOT
  > docker run -P -t -i --rm --name c1 --link seed:seed playtech/rng:1.0-SNAPSHOT
  
| name    | description
| :-----: | :---------------------------------
| -i      | interactive mode. keep STDIN open
| -t      | allocate pseudo-TTY
| -P      | publish all exposed ports to host
| --rm    | remove after stop
| --link  | link to another container by `alias:name` scheme

### JMX

Execute `bin/akka-cluster node(ip) port(9998)` to check the cluster status.