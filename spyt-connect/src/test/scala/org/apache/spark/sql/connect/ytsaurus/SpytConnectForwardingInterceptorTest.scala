package org.apache.spark.sql.connect.ytsaurus

import org.apache.spark.SparkConf
import org.apache.spark.connect.proto.{AnalyzePlanRequest, AnalyzePlanResponse, Command, CreateDataFrameViewCommand,
  ExecutePlanRequest, ExecutePlanResponse, Plan, Relation, SparkConnectServiceGrpc, SqlCommand}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sparkproject.connect.grpc.{CallOptions, ManagedChannelBuilder, Metadata, MethodDescriptor, ServerCall,
  ServerCallHandler, ServerInterceptors, ServerServiceDefinition, Status}
import org.sparkproject.connect.grpc.netty.NettyServerBuilder
import org.sparkproject.connect.grpc.stub.{ClientCalls, ServerCalls, StreamObserver}

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.jdk.CollectionConverters._

class SpytConnectForwardingInterceptorTest extends AnyFlatSpec with Matchers {

  private val createViewRequest = ExecutePlanRequest.newBuilder().setPlan(Plan.newBuilder()
    .setCommand(Command.newBuilder().setCreateDataframeView(CreateDataFrameViewCommand.getDefaultInstance))).build()
  private val sqlRequest = ExecutePlanRequest.newBuilder().setPlan(Plan.newBuilder()
    .setCommand(Command.newBuilder().setSqlCommand(SqlCommand.getDefaultInstance))).build()
  private val rootRequest = ExecutePlanRequest.newBuilder().setPlan(Plan.newBuilder()
    .setRoot(Relation.getDefaultInstance)).build()

  behavior of "SpytConnectForwardingInterceptor"

  "Constructor validation" should "reject a method configured for both forwarding modes" in {
    val conf = new SparkConf(false)
      .set(Config.YTSAURUS_CONNECT_FORWARD_METHODS.key, "AnalyzePlan")
      .set(Config.YTSAURUS_CONNECT_FORWARD_MIRROR_METHODS.key, "AnalyzePlan")
      .set(Config.YTSAURUS_CONNECT_FORWARD_ENDPOINT.key, "127.0.0.1:1")
    val error = intercept[IllegalArgumentException](new SpytConnectForwardingInterceptor(conf))
    error.getMessage should include("cannot be configured for both forwarding modes")
  }

  it should "require a nonempty endpoint for either forwarding mode" in {
    Seq(Config.YTSAURUS_CONNECT_FORWARD_METHODS.key, Config.YTSAURUS_CONNECT_FORWARD_MIRROR_METHODS.key)
      .foreach { methodKey =>
        Seq(None, Some("")).foreach { endpoint =>
          val conf = new SparkConf(false).set(methodKey, "AnalyzePlan")
          endpoint.foreach(conf.set(Config.YTSAURUS_CONNECT_FORWARD_ENDPOINT.key, _))
          withClue(s"methodKey=$methodKey, endpoint=$endpoint: ") {
            val error = intercept[IllegalArgumentException](new SpytConnectForwardingInterceptor(conf))
            error.getMessage should include("endpoint must be configured")
          }
        }
      }
  }

  "A forwarded method" should "run only on the external endpoint" in {
    checkRouting(Some(Config.YTSAURUS_CONNECT_FORWARD_METHODS.key), "external", 0, 1)
  }

  "An executed and forwarded method" should "run on both endpoints and return the local response" in {
    checkRouting(Some(Config.YTSAURUS_CONNECT_FORWARD_MIRROR_METHODS.key), "local", 1, 1)
  }

  "An unconfigured method" should "run only on the local endpoint" in {
    checkRouting(None, "local", 1, 0)
  }

  "An ExecutePlan command selector" should "forward only its matching command" in {
    val selector = Some("ExecutePlan.command.create_dataframe_view")
    checkExecutePlanRouting(createViewRequest, selector, None, "external", 0, 1)
    checkExecutePlanRouting(sqlRequest, selector, None, "local", 1, 0)
  }

  "An ExecutePlan command wildcard" should "forward every command" in {
    checkExecutePlanRouting(sqlRequest, Some("ExecutePlan.command.*"), None, "external", 0, 1)
    checkExecutePlanRouting(createViewRequest, None, Some("ExecutePlan.command.*"), "local", 1, 1)
  }

  "A forward-only ExecutePlan call" should "relay every external response" in {
    checkExecutePlanRouting(sqlRequest, Some("ExecutePlan.command.*"), None, "external", 0, 1, 2)
  }

  it should "wait for client readiness before requesting another external response" in {
    val externalServer = NettyServerBuilder.forPort(0)
      .addService(executePlanService("external", new AtomicInteger(), new CountDownLatch(0), 2)).build().start()
    val conf = new SparkConf(false)
      .set(Config.YTSAURUS_CONNECT_FORWARD_ENDPOINT.key, s"127.0.0.1:${externalServer.getPort}")
      .set(Config.YTSAURUS_CONNECT_FORWARD_METHODS.key, "ExecutePlan.root")
    val interceptor = new SpytConnectForwardingInterceptor(conf)
    val ready = new AtomicBoolean(true)
    val responses = new AtomicInteger()
    val firstResponse = new CountDownLatch(1)
    val secondResponse = new CountDownLatch(1)
    val call = new ServerCall[ExecutePlanRequest, ExecutePlanResponse] {
      override def getMethodDescriptor: MethodDescriptor[ExecutePlanRequest, ExecutePlanResponse] =
        SparkConnectServiceGrpc.getExecutePlanMethod
      override def request(numMessages: Int): Unit = ()
      override def sendHeaders(headers: Metadata): Unit = ()
      override def sendMessage(message: ExecutePlanResponse): Unit = {
        ready.set(false)
        if (responses.incrementAndGet() == 1) firstResponse.countDown() else secondResponse.countDown()
      }
      override def close(status: Status, trailers: Metadata): Unit = ()
      override def isCancelled: Boolean = false
      override def isReady: Boolean = ready.get()
    }
    val next = new ServerCallHandler[ExecutePlanRequest, ExecutePlanResponse] {
      override def startCall(
        call: ServerCall[ExecutePlanRequest, ExecutePlanResponse],
        headers: Metadata): ServerCall.Listener[ExecutePlanRequest] = fail("Forwarded request was handled locally")
    }
    try {
      val listener = interceptor.interceptCall(call, new Metadata(), next)
      listener.onMessage(rootRequest)
      listener.onHalfClose()
      firstResponse.await(5, TimeUnit.SECONDS) shouldBe true
      secondResponse.await(200, TimeUnit.MILLISECONDS) shouldBe false
      ready.set(true)
      listener.onReady()
      secondResponse.await(5, TimeUnit.SECONDS) shouldBe true
    } finally {
      interceptor.shutdown()
      externalServer.shutdownNow()
    }
  }

  it should "relay requests and responses without changing session IDs" in {
    val observedField = rootRequest.getDescriptorForType.findFieldByName("client_observed_server_side_session_id")
    val responseField = ExecutePlanResponse.getDescriptor.findFieldByName("server_side_session_id")
    val requestBuilder = rootRequest.toBuilder.setSessionId("client-session")
    if (observedField != null) requestBuilder.setField(observedField, "observed-external")
    val request = requestBuilder.build()
    val responseBuilder = ExecutePlanResponse.newBuilder().setSessionId("client-session")
    if (responseField != null) responseBuilder.setField(responseField, "external-server")
    val response = responseBuilder.build()
    val receivedRequest = new AtomicReference[ExecutePlanRequest]()
    val handler = ServerCalls.asyncServerStreamingCall[ExecutePlanRequest, ExecutePlanResponse] {
      (forwarded: ExecutePlanRequest, observer: StreamObserver[ExecutePlanResponse]) =>
        receivedRequest.set(forwarded)
        observer.onNext(response)
        observer.onCompleted()
    }
    val externalServer = NettyServerBuilder.forPort(0).addService(
      ServerServiceDefinition.builder(SparkConnectServiceGrpc.SERVICE_NAME)
        .addMethod(SparkConnectServiceGrpc.getExecutePlanMethod, handler).build()).build().start()
    val conf = new SparkConf(false)
      .set(Config.YTSAURUS_CONNECT_FORWARD_ENDPOINT.key, s"127.0.0.1:${externalServer.getPort}")
      .set(Config.YTSAURUS_CONNECT_FORWARD_METHODS.key, "ExecutePlan.root")
    val interceptor = new SpytConnectForwardingInterceptor(conf)
    val localService = executePlanService("local", new AtomicInteger(), new CountDownLatch(0))
    val localServer = NettyServerBuilder.forPort(0)
      .addService(ServerInterceptors.intercept(localService, interceptor)).build().start()
    val client = ManagedChannelBuilder.forAddress("127.0.0.1", localServer.getPort).usePlaintext().build()
    try {
      val responses = ClientCalls.blockingServerStreamingCall(
        client, SparkConnectServiceGrpc.getExecutePlanMethod,
        CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS), request).asScala.toList
      receivedRequest.get() shouldEqual request
      responses shouldEqual List(response)
    } finally {
      client.shutdownNow()
      localServer.shutdownNow()
      interceptor.shutdown()
      externalServer.shutdownNow()
    }
  }

  "An ExecutePlan root selector" should "forward root plans" in {
    checkExecutePlanRouting(rootRequest, Some("ExecutePlan.root"), None, "external", 0, 1)
    checkExecutePlanRouting(createViewRequest, Some("ExecutePlan.root"), None, "local", 1, 0)
  }

  "An ExecutePlan selector" should "take precedence over a whole-method rule" in {
    checkExecutePlanRouting(
      createViewRequest, Some("ExecutePlan"), Some("ExecutePlan.command.create_dataframe_view"), "local", 1, 1)
    checkExecutePlanRouting(
      createViewRequest, Some("ExecutePlan.command.*"), Some("ExecutePlan.command.create_dataframe_view"), "local", 1, 1)
  }

  private def checkRouting(
    methodKey: Option[String],
    expectedSessionId: String,
    expectedLocalCalls: Int,
    expectedExternalCalls: Int): Unit = {
    val localCalls = new AtomicInteger()
    val externalCalls = new AtomicInteger()
    val externalDone = new CountDownLatch(expectedExternalCalls)
    val externalServer = NettyServerBuilder.forPort(0)
      .addService(service("external", externalCalls, externalDone)).build().start()
    val conf = new SparkConf(false)
      .set(Config.YTSAURUS_CONNECT_FORWARD_ENDPOINT.key, s"127.0.0.1:${externalServer.getPort}")
    methodKey.foreach(key => conf.set(key, "AnalyzePlan"))
    val interceptor = new SpytConnectForwardingInterceptor(conf)
    val localServer = NettyServerBuilder.forPort(0)
      .addService(ServerInterceptors.intercept(service("local", localCalls, new CountDownLatch(0)), interceptor))
      .build().start()
    val client = ManagedChannelBuilder.forAddress("127.0.0.1", localServer.getPort).usePlaintext().build()
    try {
      val response = ClientCalls.blockingUnaryCall(
        client, SparkConnectServiceGrpc.getAnalyzePlanMethod, CallOptions.DEFAULT,
        AnalyzePlanRequest.getDefaultInstance)
      response.getSessionId shouldEqual expectedSessionId
      externalDone.await(5, TimeUnit.SECONDS) shouldBe true
      localCalls.get() shouldEqual expectedLocalCalls
      externalCalls.get() shouldEqual expectedExternalCalls
    } finally {
      client.shutdownNow()
      localServer.shutdownNow()
      interceptor.shutdown()
      externalServer.shutdownNow()
    }
  }

  private def service(
    sessionId: String,
    calls: AtomicInteger,
    done: CountDownLatch): ServerServiceDefinition = {
    val handler = ServerCalls.asyncUnaryCall(new ServerCalls.UnaryMethod[AnalyzePlanRequest, AnalyzePlanResponse] {
      override def invoke(request: AnalyzePlanRequest, observer: StreamObserver[AnalyzePlanResponse]): Unit = {
        calls.incrementAndGet()
        observer.onNext(AnalyzePlanResponse.newBuilder().setSessionId(sessionId).build())
        observer.onCompleted()
        done.countDown()
      }
    })
    ServerServiceDefinition.builder(SparkConnectServiceGrpc.SERVICE_NAME)
      .addMethod(SparkConnectServiceGrpc.getAnalyzePlanMethod, handler).build()
  }

  private def checkExecutePlanRouting(
    request: ExecutePlanRequest,
    forwarded: Option[String],
    mirrored: Option[String],
    expectedSessionId: String,
    expectedLocalCalls: Int,
    expectedExternalCalls: Int,
    externalResponses: Int = 1): Unit = {
    val localCalls = new AtomicInteger()
    val externalCalls = new AtomicInteger()
    val externalDone = new CountDownLatch(expectedExternalCalls)
    val externalServer = NettyServerBuilder.forPort(0)
      .addService(executePlanService("external", externalCalls, externalDone, externalResponses)).build().start()
    val conf = new SparkConf(false)
      .set(Config.YTSAURUS_CONNECT_FORWARD_ENDPOINT.key, s"127.0.0.1:${externalServer.getPort}")
    forwarded.foreach(conf.set(Config.YTSAURUS_CONNECT_FORWARD_METHODS.key, _))
    mirrored.foreach(conf.set(Config.YTSAURUS_CONNECT_FORWARD_MIRROR_METHODS.key, _))
    val interceptor = new SpytConnectForwardingInterceptor(conf)
    val localServer = NettyServerBuilder.forPort(0)
      .addService(ServerInterceptors.intercept(
        executePlanService("local", localCalls, new CountDownLatch(0)), interceptor)).build().start()
    val client = ManagedChannelBuilder.forAddress("127.0.0.1", localServer.getPort).usePlaintext().build()
    try {
      val responses = ClientCalls.blockingServerStreamingCall(
        client,
        SparkConnectServiceGrpc.getExecutePlanMethod,
        CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS), request).asScala.toList
      responses.map(_.getSessionId) shouldEqual List.fill(if (expectedExternalCalls > 0 && expectedLocalCalls == 0) {
        externalResponses
      } else {
        1
      })(expectedSessionId)
      externalDone.await(5, TimeUnit.SECONDS) shouldBe true
      localCalls.get() shouldEqual expectedLocalCalls
      externalCalls.get() shouldEqual expectedExternalCalls
    } finally {
      client.shutdownNow()
      localServer.shutdownNow()
      interceptor.shutdown()
      externalServer.shutdownNow()
    }
  }

  private def executePlanService(
    sessionId: String,
    calls: AtomicInteger,
    done: CountDownLatch,
    responses: Int = 1): ServerServiceDefinition = {
    val handler = ServerCalls.asyncServerStreamingCall(
      new ServerCalls.ServerStreamingMethod[ExecutePlanRequest, ExecutePlanResponse] {
        override def invoke(request: ExecutePlanRequest, observer: StreamObserver[ExecutePlanResponse]): Unit = {
          calls.incrementAndGet()
          (0 until responses).foreach { _ =>
            observer.onNext(ExecutePlanResponse.newBuilder().setSessionId(sessionId).build())
          }
          observer.onCompleted()
          done.countDown()
        }
      })
    ServerServiceDefinition.builder(SparkConnectServiceGrpc.SERVICE_NAME)
      .addMethod(SparkConnectServiceGrpc.getExecutePlanMethod, handler).build()
  }
}
