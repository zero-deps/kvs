# Abstract scala type database.

Key Value Storage.

# Backend

 * Ring http://gitlab.ee.playtech.corp/mws/rng
 * Memory
 * FS
 * etc.

# Services Handlers

Implementation of particular data types storage with some service specific logic.


Reference configuration

#
# Defaults for the KVS database.
#
kvs {
  # KVS storage
  # Backend storage engine of KVS abstract scala types storage.
  # Memory       - for memory
  # fs        - for file system
  # sql       - for SQL
  # Leveldb   - for leveldb
  # Ring       - for RNG distributed consistent hash storage
  store="mws.kvs.store.Ring"

  # Value says minimum difference in time between first and last message.
  # Also, in same time interval values will be deleted.
  # That all means that first message can be older than last from (value) to (2*value).
  message.time-diff = 5s

  # KVS System Feeds
  # The containers for holding the system maintaned feeds to create on startup.
  # Some kind of db schema. Possible values (maybe fqcns?):
  # - users
  # - groups
  # - games
  # - products
  # - rooms
  # - etc.
  # Property may be moved to Feeds Server.
  feeds=["users"]
}

leveldb {
  # Storage location of LevelDB files
  dir = storage
  # Use fsync on write
  fsync = on
  # Verify checksum on read
  checksum = off
  # Native LevelDB (via JNI) or LevelDB Java port
  native = on
}
