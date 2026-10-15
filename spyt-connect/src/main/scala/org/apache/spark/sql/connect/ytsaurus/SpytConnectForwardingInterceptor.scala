package org.apache.spark.sql.connect.ytsaurus

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.connect.proto.{Command, ExecutePlanRequest, SparkConnectServiceGrpc}
import org.slf4j.LoggerFactory
import org.sparkproject.connect.grpc.{CallOptions, ClientCall, Context, ManagedChannel, ManagedChannelBuilder, Metadata,
  ServerCall, ServerCallHandler, ServerInterceptor, Status}

import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

object SpytConnectForwardingInterceptor {
  private[ytsaurus] case class ForwardingConfig(
    forwardedMethods: Set[String],
    mirroredMethods: Set[String],
    endpoint: Option[String])

  private[ytsaurus] def validateConfig(
    forwardMethods: Option[String],
    mirrorMethods: Option[String],
    endpoint: Option[String]): ForwardingConfig = {
    val forwardedMethods = configuredMethods(forwardMethods)
    val mirroredMethods = configuredMethods(mirrorMethods)

    require(
      forwardedMethods.isEmpty && mirroredMethods.isEmpty || endpoint.exists(_.nonEmpty),
      "A Spark Connect forwarding endpoint must be configured when forwarding methods are specified")
    require(
      (forwardedMethods intersect mirroredMethods).isEmpty,
      "A Spark Connect gRPC method cannot be configured for both forwarding modes")

    ForwardingConfig(forwardedMethods, mirroredMethods, endpoint)
  }

  private def configuredMethods(value: Option[String]): Set[String] = {
    val available = SparkConnectServiceGrpc.getServiceDescriptor.getMethods.asScala.map { descriptor =>
      descriptor.getBareMethodName -> descriptor.getFullMethodName
    }.toMap
    value.toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty).map { method =>
      if (method.startsWith("ExecutePlan.")) {
        val selector = method.stripPrefix("ExecutePlan.")
        val commandCases = Command.CommandTypeCase.values().iterator.map(_.name().toLowerCase(Locale.ROOT)).toSet
        val valid = selector == "root" || selector == "command.*" ||
          selector.startsWith("command.") && commandCases.contains(selector.stripPrefix("command."))
        require(valid, s"Unknown Spark Connect ExecutePlan selector: $method")
        method
      } else {
        available.getOrElse(method, {
          require(available.valuesIterator.contains(method), s"Unknown Spark Connect gRPC method: $method")
          method
        })
      }
    }.toSet
  }
}

class SpytConnectForwardingInterceptor private[ytsaurus] (sparkConf: SparkConf) extends ServerInterceptor {

  def this() = this(SparkEnv.get.conf)

  private val log = LoggerFactory.getLogger(getClass)
  private val cfg = SpytConnectForwardingInterceptor.validateConfig(
    sparkConf.get(Config.YTSAURUS_CONNECT_FORWARD_METHODS),
    sparkConf.get(Config.YTSAURUS_CONNECT_FORWARD_MIRROR_METHODS),
    sparkConf.get(Config.YTSAURUS_CONNECT_FORWARD_ENDPOINT))
  private val executePlanMethod = SparkConnectServiceGrpc.getExecutePlanMethod.getFullMethodName
  private val hasPlanSelectors = (cfg.forwardedMethods ++ cfg.mirroredMethods).exists(_.startsWith("ExecutePlan."))

  private val channel: Option[ManagedChannel] = cfg.endpoint.filter { _ =>
    cfg.forwardedMethods.nonEmpty || cfg.mirroredMethods.nonEmpty
  }.map { address =>
    ManagedChannelBuilder.forTarget(address).usePlaintext().build()
  }

  channel.foreach { managedChannel =>
    Runtime.getRuntime.addShutdownHook(new Thread(() => managedChannel.shutdown(), "Spark Connect forwarding shutdown"))
  }

  private[ytsaurus] def shutdown(): Unit = channel.foreach(_.shutdownNow())

  override def interceptCall[ReqT, RespT](
    call: ServerCall[ReqT, RespT],
    headers: Metadata,
    next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    val method = call.getMethodDescriptor.getFullMethodName
    if (method == executePlanMethod && hasPlanSelectors) {
      call.request(1)
      return new ServerCall.Listener[ReqT] {
        private var delegate: ServerCall.Listener[ReqT] = _

        override def onMessage(message: ReqT): Unit = {
          if (delegate == null) {
            val mode = message match {
              case request: ExecutePlanRequest => executePlanMode(request)
              case _ => None
            }
            delegate = mode match {
              case Some(executeLocally) => startForwarding(call, headers, next, executeLocally, false)
              case None => next.startCall(call, headers)
            }
          }
          delegate.onMessage(message)
        }

        override def onHalfClose(): Unit = {
          if (delegate == null) delegate = next.startCall(call, headers)
          delegate.onHalfClose()
        }

        override def onCancel(): Unit = if (delegate != null) delegate.onCancel()

        override def onComplete(): Unit = if (delegate != null) delegate.onComplete()

        override def onReady(): Unit = if (delegate != null) delegate.onReady()
      }
    }

    if (cfg.forwardedMethods.contains(method) || cfg.mirroredMethods.contains(method)) {
      startForwarding(call, headers, next, cfg.mirroredMethods.contains(method), true)
    } else {
      next.startCall(call, headers)
    }
  }

  private def startForwarding[ReqT, RespT](
    call: ServerCall[ReqT, RespT],
    headers: Metadata,
    next: ServerCallHandler[ReqT, RespT],
    executeLocally: Boolean,
    requestFirstMessage: Boolean): ServerCall.Listener[ReqT] = {
    val method = call.getMethodDescriptor.getFullMethodName
    val localListener = if (executeLocally) Some(next.startCall(call, headers)) else None
    val responseAwaitingReady = new AtomicBoolean(false)
    val clientCall = Context.ROOT.call(() => channel.get.newCall(call.getMethodDescriptor, CallOptions.DEFAULT))
    val forwardedHeaders = new Metadata()
    forwardedHeaders.merge(headers)
    val responseListener = if (executeLocally) {
      new ClientCall.Listener[RespT] {
        override def onMessage(message: RespT): Unit = clientCall.request(1)

        override def onClose(status: Status, trailers: Metadata): Unit = {
          if (!status.isOk) log.warn(s"Forwarded Spark Connect method $method failed: $status")
        }
      }
    } else {
      new ClientCall.Listener[RespT] {
        override def onHeaders(responseHeaders: Metadata): Unit = call.sendHeaders(responseHeaders)

        override def onMessage(message: RespT): Unit = {
          call.sendMessage(message)
          if (call.isReady) {
            clientCall.request(1)
          } else {
            responseAwaitingReady.set(true)
            if (call.isReady && responseAwaitingReady.compareAndSet(true, false)) clientCall.request(1)
          }
        }

        override def onClose(status: Status, trailers: Metadata): Unit = call.close(status, trailers)
      }
    }
    Context.ROOT.run(() => clientCall.start(responseListener, forwardedHeaders))
    clientCall.request(1)
    if (requestFirstMessage) call.request(1)

    new ServerCall.Listener[ReqT] {
      override def onMessage(message: ReqT): Unit = {
        clientCall.sendMessage(message)
        localListener.foreach(_.onMessage(message))
        call.request(1)
      }

      override def onHalfClose(): Unit = {
        clientCall.halfClose()
        localListener.foreach(_.onHalfClose())
      }

      override def onCancel(): Unit = {
        localListener.foreach(_.onCancel())
        clientCall.cancel("Spark Connect client cancelled the request", null)
      }

      override def onComplete(): Unit = localListener.foreach(_.onComplete())

      override def onReady(): Unit = {
        if (!executeLocally && call.isReady && responseAwaitingReady.compareAndSet(true, false)) {
          clientCall.request(1)
        }
        localListener.foreach(_.onReady())
      }
    }
  }

  private def executePlanMode(request: ExecutePlanRequest): Option[Boolean] = {
    val plan = request.getPlan
    val selector = if (plan.hasRoot) {
      Some("ExecutePlan.root")
    } else if (plan.hasCommand) {
      Some(s"ExecutePlan.command.${plan.getCommand.getCommandTypeCase.name().toLowerCase(Locale.ROOT)}")
    } else {
      None
    }
    val forwardedScore = selector.flatMap(ruleScore(cfg.forwardedMethods, _))
      .orElse(if (cfg.forwardedMethods.contains(executePlanMethod)) Some(0) else None)
    val mirroredScore = selector.flatMap(ruleScore(cfg.mirroredMethods, _))
      .orElse(if (cfg.mirroredMethods.contains(executePlanMethod)) Some(0) else None)
    (forwardedScore, mirroredScore) match {
      case (Some(forwarded), Some(mirrored)) => Some(mirrored > forwarded)
      case (Some(_), None) => Some(false)
      case (None, Some(_)) => Some(true)
      case (None, None) => None
    }
  }

  private def ruleScore(rules: Set[String], selector: String): Option[Int] = {
    rules.iterator.filter(_.startsWith("ExecutePlan.")).flatMap { rule =>
      if (rule.endsWith("*") && selector.startsWith(rule.dropRight(1))) {
        Some(rule.length - 1)
      } else if (rule == selector) {
        Some(rule.length)
      } else {
        None
      }
    }.toSeq.sorted.lastOption
  }
}
