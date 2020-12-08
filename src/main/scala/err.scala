package kvs

sealed trait Err
case class AckFail          (   a: Ack    ) extends Err
case class EntryExists      ( key: EnKey  ) extends Err
case class FileNotExists    (path: PathKey) extends Err
case class FileAlreadyExists(path: PathKey) extends Err
