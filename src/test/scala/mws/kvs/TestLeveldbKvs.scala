package mws.kvs

import akka.actor.Props
import com.typesafe.config.Config
import org.iq80.leveldb.DB

object TestLeveldbKvs {
  def props(leveldb: DB, config: Config): Props =
    Props(new TestLeveldbKvs(leveldb, config))
}

class TestLeveldbKvs(val db: DB,
                     val config: Config) extends TestKvs
  with InMemoryKvs
