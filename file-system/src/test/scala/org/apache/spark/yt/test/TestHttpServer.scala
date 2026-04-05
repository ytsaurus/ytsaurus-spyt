package org.apache.spark.yt.test

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http._
import org.apache.spark.yt.test.TestHttpServer.{Request, Response}
import org.slf4j.{Logger, LoggerFactory}
import tech.ytsaurus.spyt.wrapper.YtJavaConverters.toScalaDuration

import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.time.Duration
import scala.concurrent.{Await, Promise}
import scala.util.control.NonFatal

trait TestHttpServer extends AutoCloseable {
  def start(): Unit
  def stop(): Unit
  def port: Int

  def expect(check: Request => Boolean): TestHttpServer
  def assert(check: Request => Unit): TestHttpServer = expect(check.andThen(_ => true))
  def respond(response: Response): TestHttpServer
  def awaitResult(duration: Duration = Duration.ofSeconds(5)): Response

  override def close(): Unit = stop()
}

object TestHttpServer {
  val log: Logger = LoggerFactory.getLogger(TestHttpServer.getClass)

  case class Response(body: Array[Byte], contentType: String, httpStatusCode: Int = 200, httpStatusLine: String = "OK")
  case class Request(body: String, contentType: String)

  val OK: Response = Response("OK".getBytes(Charset.defaultCharset()), contentType = "text/plain")

  def apply(): TestHttpServer = new TestHttpServer {
    private var serverChannelFuture: ChannelFuture = _
    private var bossGroup: EventLoopGroup = _
    private var workerGroup: EventLoopGroup = _

    private var assignedPort: Int = -1

    private var respond: Response = OK
    private var expected: (Request => Boolean) = r => true

    private var promise: Promise[Response] = Promise()

    def start(): Unit = {
      bossGroup = new NioEventLoopGroup(1)
      workerGroup = new NioEventLoopGroup

      val requestHandler = new ChannelInboundHandlerAdapter() {
        override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
          case request: FullHttpRequest =>
            val req = Request(
              body = request.content().toString(Charset.defaultCharset()),
              contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE)
            )
            log.info(s"Got request: $req")

            val response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
            response.headers.set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8")
            response.headers.set(HttpHeaderNames.CONTENT_LENGTH, 0)
            ctx.writeAndFlush(response)

            try {
              expected(req)
              promise.success(respond)
            } catch {
              case NonFatal(ex) => promise.failure(ex)
            }

          case _ => ctx.fireChannelRead(msg)
        }
      }

      val bootstrap = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new ChannelInitializer[SocketChannel]() {
          override protected def initChannel(ch: SocketChannel): Unit = {
            val pipeline = ch.pipeline
            pipeline.addLast(new HttpRequestDecoder)
            pipeline.addLast(new HttpObjectAggregator(10 * 1024 * 1024))
            pipeline.addLast(new HttpResponseEncoder)

            pipeline.addLast(requestHandler)
          }
        })
        .childOption(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)

      serverChannelFuture = bootstrap.bind(0).sync
      assignedPort = serverChannelFuture.channel().localAddress().asInstanceOf[InetSocketAddress].getPort
      log.info(s"Started test server on port $assignedPort")
    }

    override def stop(): Unit = {
      if (serverChannelFuture != null) {
        serverChannelFuture.channel.close
        serverChannelFuture = null
      }
      if (bossGroup != null) {
        bossGroup.shutdownGracefully()
        bossGroup = null
      }
      if (workerGroup != null) {
        workerGroup.shutdownGracefully()
        workerGroup = null
      }
    }

    override def expect(check: Request => Boolean): TestHttpServer = {
      expected = check
      this
    }

    override def respond(response: Response): TestHttpServer = {
      respond = response
      this
    }

    override def awaitResult(duration: Duration): Response = {
      try {
        Await.result(promise.future, toScalaDuration(duration))
      } finally {
        promise = Promise()
      }
    }

    override def port: Int = assignedPort
  }
}