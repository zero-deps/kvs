package kvs.rng

import scala.concurrent.*, duration.*
import scala.language.postfixOps

case class Quorum
  ( N: Int
  , W: Int
  , R: Int
  )

case class Conf(
  quorum: Quorum = Quorum(N=1, W=1, R=1)
, buckets:      Int = 32768 /* 2^15 */
, virtualNodes: Int =   128
, hashLength:   Int =    32
, ringTimeout:   FiniteDuration = 11 seconds /* bigger than gatherTimeout */
, gatherTimeout: FiniteDuration = 10 seconds
, dumpTimeout:   FiniteDuration =  1 hour
, replTimeout:   FiniteDuration =  1 minute
, dir: String = "data_rng"
)

def pekkoConf(name: String, host: String, port: Int): String = s"""
  pekko {
    actor {
      provider = cluster
      deployment {
        /ring_readonly_store {
          router = round-robin-pool
          nr-of-instances = 5
        }
      }
      debug {
        receive = off
        lifecycle = off
      }
      serializers {
        kvsproto = kvs.rng.Serializer
      }
      serialization-identifiers {
        "kvs.rng.Serializer" = 50
      }
      serialization-bindings {
        "kvs.rng.model.ChangeState"         = kvsproto
        "kvs.rng.model.StoreGetAck"         = kvsproto
        "kvs.rng.model.StoreDelete"         = kvsproto
        "kvs.rng.model.StoreGet"            = kvsproto
        "kvs.rng.model.StorePut"            = kvsproto
        "kvs.rng.model.DumpBucketData"      = kvsproto
        "kvs.rng.model.DumpGetBucketData"   = kvsproto
        "kvs.rng.model.ReplBucketPut"       = kvsproto
        "kvs.rng.model.ReplBucketUpToDate"  = kvsproto
        "kvs.rng.model.ReplGetBucketIfNew"  = kvsproto
        "kvs.rng.model.ReplNewerBucketData" = kvsproto
      }
    }
    remote.artery.canonical {
      hostname = $host
      port = $port
    }
    cluster.seed-nodes = [ "pekko://$name@$host:$port" ]
  }
  """
