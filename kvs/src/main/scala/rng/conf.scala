package zd.rng

import scala.concurrent.*, duration.*

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
, ringTimeout:   FiniteDuration = 11.seconds /* bigger than gatherTimeout */
, gatherTimeout: FiniteDuration = 10.seconds
, dumpTimeout:   FiniteDuration =  1.hour
, replTimeout:   FiniteDuration =  1.minute
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
        kvsproto = zd.rng.Serializer
      }
      serialization-identifiers {
        "zd.rng.Serializer" = 50
      }
      serialization-bindings {
        "zd.rng.model.ChangeState"         = kvsproto
        "zd.rng.model.StoreGetAck"         = kvsproto
        "zd.rng.model.StoreDelete"         = kvsproto
        "zd.rng.model.StoreGet"            = kvsproto
        "zd.rng.model.StorePut"            = kvsproto
        "zd.rng.model.DumpBucketData"      = kvsproto
        "zd.rng.model.DumpGetBucketData"   = kvsproto
        "zd.rng.model.ReplBucketPut"       = kvsproto
        "zd.rng.model.ReplBucketUpToDate"  = kvsproto
        "zd.rng.model.ReplGetBucketIfNew"  = kvsproto
        "zd.rng.model.ReplNewerBucketData" = kvsproto
      }
    }
    remote.artery.canonical {
      hostname = $host
      port = $port
    }
    cluster.seed-nodes = [ "pekko://$name@$host:$port" ]
  }
  """
