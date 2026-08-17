package zd.kvs

import akka.actor.{Actor, Props}
import proto.*
import zio.*

/** Serializes operations sharing the same entity ID through one shard entity. */
trait SeqConsistency:
  def send[A](msg: Any): IO[Err, A]
end SeqConsistency

object SeqConsistency:
  final class Config private[kvs] (
    val name: String
  , private[kvs] val handler: Any => IO[Err, Any]
  , private[kvs] val entityId: Any => String
  , private[kvs] val encodeRequest: Any => Array[Byte]
  , private[kvs] val decodeRequest: Array[Byte] => Any
  , private[kvs] val encodeResponse: Any => Array[Byte]
  , private[kvs] val decodeResponse: Array[Byte] => Any
  )

  object Config:
    def apply[Command: MessageCodec, Response: MessageCodec](
      name: String
    , handler: Command => IO[Err, Response]
    , entityId: Command => String
    ): Config =
      new Config(
        name
      , msg => handler(msg.asInstanceOf[Command]).map(identity[Any])
      , msg => entityId(msg.asInstanceOf[Command])
      , msg => encode(msg.asInstanceOf[Command])
      , bytes => decode[Command](bytes)
      , value => encode(value.asInstanceOf[Response])
      , bytes => decode[Response](bytes)
      )
  end Config

  val layer: RLayer[ClusterSharding & Config, SeqConsistency] =
    ZLayer:
      for
        sharding <- ZIO.service[ClusterSharding]
        config <- ZIO.service[Config]
        shards <- sharding.start(
          config.name
        , Props(new Actor:
            def receive: Receive =
              case request: ShardRequest =>
                val replyTo = sender()
                val response =
                  ZIO
                    .attempt(config.decodeRequest(request.payload))
                    .orDie
                    .flatMap(config.handler)
                    .flatMap(value => ZIO.attempt(config.encodeResponse(value)).orDie)
                    .foldCause(
                      cause => cause.failureOption match
                        case Some(error) => ShardFailure(encodeError(error))
                        case None => ShardDefect(cause.prettyPrint)
                    , ShardSuccess(_)
                    )
                replyTo ! Unsafe.unsafely(Runtime.default.unsafe.run(response).getOrThrowFiberFailure())
          )
        )
      yield
        new SeqConsistency:
          def send[A](msg: Any): IO[Err, A] =
            for
              request <- ZIO.attempt:
                ShardRequest(config.entityId(msg), config.encodeRequest(msg))
              .orDie
              response <- sharding.send(shards, request)
              result <- response match
                case ShardSuccess(payload) =>
                  ZIO.attempt(config.decodeResponse(payload).asInstanceOf[A]).orDie
                case ShardFailure(error) => ZIO.fail(decodeError(error))
                case ShardDefect(message) => ZIO.dieMessage(message)
                case value => ZIO.dieMessage(s"Unexpected sharding response: $value")
            yield result

  private def encodeError(error: Err): WireError =
    error match
      case EntryExists(key) => WireError("EntryExists", key :: Nil)
      case KeyNotFound => WireError("KeyNotFound", Nil)
      case FileNotExists(dir, name) => WireError("FileNotExists", dir :: name :: Nil)
      case FileAlreadyExists(dir, name) => WireError("FileAlreadyExists", dir :: name :: Nil)
      case Fail(reason) => WireError("Fail", reason :: Nil)
      case Failed(error) => WireError("Failed", error.getClass.getName :: Option(error.getMessage).getOrElse("") :: Nil)
      case InvalidArgument(description) => WireError("InvalidArgument", description :: Nil)
      case RngAskQuorumFailed(why) => WireError("RngAskQuorumFailed", why :: Nil)
      case RngAskTimeoutFailed(operation, key) => WireError("RngAskTimeoutFailed", operation :: key :: Nil)
      case RngFail(message) => WireError("RngFail", message :: Nil)

  private def decodeError(error: WireError): Err =
    (error.code, error.values) match
      case ("EntryExists", key :: Nil) => EntryExists(key)
      case ("KeyNotFound", Nil) => KeyNotFound
      case ("FileNotExists", dir :: name :: Nil) => FileNotExists(dir, name)
      case ("FileAlreadyExists", dir :: name :: Nil) => FileAlreadyExists(dir, name)
      case ("Fail", reason :: Nil) => Fail(reason)
      case ("Failed", className :: message :: Nil) => Failed(RuntimeException(s"$className: $message"))
      case ("InvalidArgument", description :: Nil) => InvalidArgument(description)
      case ("RngAskQuorumFailed", why :: Nil) => RngAskQuorumFailed(why)
      case ("RngAskTimeoutFailed", operation :: key :: Nil) => RngAskTimeoutFailed(operation, key)
      case ("RngFail", message :: Nil) => RngFail(message)
      case _ => throw IllegalArgumentException(s"Invalid sharding error payload: $error")
end SeqConsistency
