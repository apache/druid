/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.java.util.http.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.pool.ResourceContainer;
import org.apache.druid.java.util.http.client.pool.ResourcePool;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.joda.time.Duration;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link HttpClient} built using Netty.
 */
public class NettyHttpClient extends AbstractHttpClient
{
  private static final Logger log = new Logger(NettyHttpClient.class);

  private static final String READ_TIMEOUT_HANDLER_NAME = "read-timeout";
  private static final String LAST_HANDLER_NAME = "last-handler";

  private final Timer timer;
  private final ResourcePool<String, ChannelFuture> pool;
  private final HttpClientConfig.CompressionCodec compressionCodec;
  private final Duration defaultReadTimeout;
  private long backPressureStartTimeNs;

  NettyHttpClient(
      ResourcePool<String, ChannelFuture> pool,
      Duration defaultReadTimeout,
      HttpClientConfig.CompressionCodec compressionCodec,
      Timer timer
  )
  {
    this.pool = Preconditions.checkNotNull(pool, "pool");
    this.defaultReadTimeout = defaultReadTimeout;
    this.compressionCodec = Preconditions.checkNotNull(compressionCodec);
    this.timer = timer;

    if (defaultReadTimeout != null && defaultReadTimeout.getMillis() > 0) {
      Preconditions.checkNotNull(timer, "timer");
    }
  }

  @LifecycleStart
  public void start()
  {
  }

  @LifecycleStop
  public void stop()
  {
    // Mark as closed but let in-flight requests complete
    // Don't wait - this allows shutdown to proceed without interrupting active requests
    pool.close();
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      final Request request,
      final HttpResponseHandler<Intermediate, Final> handler,
      final Duration requestReadTimeout
  )
  {
    final HttpMethod method = request.getMethod();
    final URL url = request.getUrl();
    final Multimap<String, String> headers = request.getHeaders();

    final String requestDesc = method + " " + url;
    if (log.isDebugEnabled()) {
      log.debug("[%s] starting", requestDesc);
    }

    // Block while acquiring a channel from the pool, then complete the request asynchronously.
    final Channel channel;
    final String hostKey = getPoolKey(url);
    final ResourceContainer<ChannelFuture> channelResourceContainer = pool.take(hostKey);

    // Handle pool exhaustion - take() returns null if pool is exhausted or timed out
    if (channelResourceContainer == null) {
      return Futures.immediateFailedFuture(
          new ChannelException(
              "Connection pool exhausted or timed out for host: " + hostKey
          )
      );
    }

    final ChannelFuture channelFuture = channelResourceContainer.get().awaitUninterruptibly();
    if (!channelFuture.isSuccess()) {
      channelResourceContainer.returnResource(); // Some other poor sap will have to deal with it...
      return Futures.immediateFailedFuture(
          new ChannelException(
              "Faulty channel in resource pool",
              channelFuture.cause()
          )
      );
    } else {
      channel = channelFuture.channel();

      // In case we get a channel that never had its readability turned back on.
      channel.config().setAutoRead(true);
    }
    final String urlFile = StringUtils.nullToEmptyNonDruidDataString(url.getFile());

    // Give Netty its own view of the body rather than the caller's buffer. Netty's HttpObjectEncoder
    // releases whatever it encodes, and writing to the socket advances the reader index, so passing
    // request.getContent() directly would leave the caller holding a released, fully consumed buffer.
    // Callers do reuse a Request: KerberosHttpClient resends request.copy() after a 401, and
    // ClientUtils copies a request's content to retarget it at another server. A retained duplicate
    // shares the bytes but has its own indices, so Netty's release balances our retain and the
    // original is left untouched.
    final ByteBuf content = request.hasContent()
                            ? request.getContent().retainedDuplicate()
                            : Unpooled.EMPTY_BUFFER;
    final DefaultFullHttpRequest httpRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        method,
        urlFile.isEmpty() ? "/" : urlFile,
        content
    );

    if (!headers.containsKey(HttpHeaderNames.HOST.toString())) {
      httpRequest.headers().add(HttpHeaderNames.HOST, getHost(url));
    }

    // If Accept-Encoding is set in the Request, use that. Otherwise use the default from "compressionCodec".
    if (!headers.containsKey(HttpHeaderNames.ACCEPT_ENCODING.toString())) {
      httpRequest.headers().set(HttpHeaderNames.ACCEPT_ENCODING, compressionCodec.getEncodingString());
    }

    for (Map.Entry<String, Collection<String>> entry : headers.asMap().entrySet()) {
      String key = entry.getKey();

      for (String obj : entry.getValue()) {
        httpRequest.headers().add(key, obj);
      }
    }

    final long readTimeout = getReadTimeout(requestReadTimeout);
    final SettableFuture<Final> retVal = SettableFuture.create();

    // Pipeline can hand us chunks even after exceptionCaught is called. This has the potential to confuse
    // HttpResponseHandler implementations, which expect exceptionCaught to be the final method called. So, we
    // use this boolean to ensure that handlers do not see any chunks after exceptionCaught fires.
    final AtomicBoolean didEncounterException = new AtomicBoolean();

    if (readTimeout > 0) {
      // Netty 4's ReadTimeoutHandler schedules its timeout on the channel's event loop, whose blocking
      // epoll_wait/select can be interrupted by signals (e.g. a JFR/profiler agent), which resets the wait
      // and causes scheduled timeouts to be delayed or missed on some JDKs. To match the pre-Netty-4 behavior
      // (which drove read timeouts off a dedicated HashedWheelTimer thread), we schedule the read timeout on
      // the shared Timer instead of the event loop.
      channel.pipeline().addLast(
          READ_TIMEOUT_HANDLER_NAME,
          new TimerReadTimeoutHandler(timer, readTimeout)
      );
    }

    channel.pipeline().addLast(
        LAST_HANDLER_NAME,
        new ChannelInboundHandlerAdapter()
        {
          private volatile ClientResponse<Intermediate> response = null;

          // Chunk number most recently assigned.
          private long currentChunkNum = 0;

          // Suspend and resume watermarks (respectively: last chunk number that triggered a suspend, and that was
          // provided to the TrafficCop's resume method). Synchronized access since they are not always accessed
          // from an I/O thread. (TrafficCops can be called from any thread.)
          private final Object watermarkLock = new Object();
          private long suspendWatermark = -1;
          private long resumeWatermark = -1;

          @Override
          public void channelRead(ChannelHandlerContext ctx, Object msg)
          {
            if (log.isDebugEnabled()) {
              log.debug("[%s] messageReceived: %s", requestDesc, msg);
            }
            try {
              // Unlike Netty 3 (which threw during decoding), Netty 4's HTTP decoder does not throw on a malformed
              // response: it emits a message flagged with a failed DecoderResult. Surface that failure as an
              // exception so callers see the underlying cause (e.g. "invalid version format") instead of silently
              // proceeding until the channel disconnects.
              if (msg instanceof HttpObject) {
                final DecoderResult decoderResult = ((HttpObject) msg).decoderResult();
                if (decoderResult.isFailure()) {
                  handleExceptionAndCloseChannel(decoderResult.cause(), false);
                  return;
                }
              }

              if (msg instanceof HttpResponse) {
                if (didEncounterException.get()) {
                  // Don't process HttpResponse after encountering an exception.
                  return;
                }

                HttpResponse httpResponse = (HttpResponse) msg;
                if (log.isDebugEnabled()) {
                  log.debug("[%s] Got response: %s", requestDesc, httpResponse.status());
                }

                HttpResponseHandler.TrafficCop trafficCop = new HttpResponseHandler.TrafficCop()
                {
                  @Override
                  public long resume(long resumeChunkNum)
                  {
                    synchronized (watermarkLock) {
                      resumeWatermark = Math.max(resumeWatermark, resumeChunkNum);

                      if (suspendWatermark >= 0 && resumeWatermark >= suspendWatermark) {
                        suspendWatermark = -1;
                        channel.config().setAutoRead(true);
                        long backPressureDuration = System.nanoTime() - backPressureStartTimeNs;
                        log.debug("[%s] Resumed reads from channel (chunkNum = %,d).", requestDesc, resumeChunkNum);
                        return backPressureDuration;
                      }
                    }

                    return 0; //If we didn't resume, don't know if backpressure was happening
                  }

                  @Override
                  public void abort()
                  {
                    log.debug("[%s] Aborted connection at caller's request.", requestDesc);
                    channel.close();
                  }
                };
                response = handler.handleResponse(httpResponse, trafficCop);
                if (response.isFinished()) {
                  retVal.set((Final) response.getObj());
                }

                assert currentChunkNum == 0;
                possiblySuspendReads(response);

                if (msg instanceof LastHttpContent) {
                  finishRequest();
                }
              } else if (msg instanceof HttpContent) {
                if (didEncounterException.get()) {
                  // Don't process HttpChunk after encountering an exception.
                  return;
                }

                HttpContent httpChunk = (HttpContent) msg;
                if (log.isDebugEnabled()) {
                  log.debug(
                      "[%s] Got chunk: %sB, last=%s",
                      requestDesc,
                      httpChunk.content().readableBytes(),
                      msg instanceof LastHttpContent
                  );
                }

                response = handler.handleChunk(response, httpChunk, ++currentChunkNum);
                if (response.isFinished() && !retVal.isDone()) {
                  retVal.set((Final) response.getObj());
                }
                possiblySuspendReads(response);

                if (msg instanceof LastHttpContent) {
                  finishRequest();
                }
              } else {
                throw new ISE("Unknown message type[%s]", msg.getClass());
              }
            }
            catch (Exception ex) {
              log.warn(ex, "[%s] Exception thrown while processing message, closing channel.", requestDesc);

              if (!retVal.isDone()) {
                retVal.set(null);
              }
              channel.close();
              channelResourceContainer.returnResource();

              throw ex;
            }
            finally {
              // Netty 4 requires inbound messages to be released once handled, and
              // ChannelInboundHandlerAdapter (unlike SimpleChannelInboundHandler) does not do it for us. Release
              // here rather than per branch so that the message is freed even when a handler throws: a response
              // that trips a limit or a timeout inside handleResponse/handleChunk would otherwise leak its
              // pooled buffer. Handlers are expected to have copied or retained whatever they still need.
              ReferenceCountUtil.release(msg);
            }
          }

          private void possiblySuspendReads(ClientResponse<?> response)
          {
            if (!response.isContinueReading()) {
              synchronized (watermarkLock) {
                suspendWatermark = Math.max(suspendWatermark, currentChunkNum);
                if (suspendWatermark > resumeWatermark) {
                  channel.config().setAutoRead(false);
                  backPressureStartTimeNs = System.nanoTime();
                  log.debug("[%s] Suspended reads from channel (chunkNum = %,d).", requestDesc, currentChunkNum);
                }
              }
            }
          }

          private void finishRequest()
          {
            ClientResponse<Final> finalResponse = handler.done(response);

            if (!finalResponse.isFinished() || !finalResponse.isContinueReading()) {
              throw new ISE(
                  "[%s] Didn't get a completed ClientResponse Object from [%s] (finished = %s, continueReading = %s)",
                  requestDesc,
                  handler.getClass(),
                  finalResponse.isFinished(),
                  finalResponse.isContinueReading()
              );
            }
            if (!retVal.isDone()) {
              retVal.set(finalResponse.getObj());
            }
            removeHandlers();
            channel.config().setAutoRead(true);
            channelResourceContainer.returnResource();
          }

          @Override
          public void exceptionCaught(ChannelHandlerContext context, Throwable cause)
          {
            handleExceptionAndCloseChannel(cause, false);
          }

          @Override
          public void channelInactive(ChannelHandlerContext context)
          {
            handleExceptionAndCloseChannel(new ChannelException("Channel disconnected"), true);
          }

          /**
           * Handle an exception by logging it, possibly calling {@link SettableFuture#setException} on {@code retVal},
           * possibly calling {@link HttpResponseHandler#exceptionCaught}, and possibly closing the channel.
           *
           * No actions will be taken (other than logging) if an exception has already been handled for this request.
           *
           * @param t exception
           * @param closeIfNotOpen Call {@link Channel#close()} even if {@link Channel#isOpen()} returns false.
           *                       Provided to retain existing behavior of two different chunks of code that were
           *                       merged into this single method.
           */
          private void handleExceptionAndCloseChannel(final Throwable t, final boolean closeIfNotOpen)
          {
            if (log.isDebugEnabled()) {
              log.debug(t, "[%s] Caught exception", requestDesc);
            }

            // Only process the first exception encountered.
            if (!didEncounterException.compareAndSet(false, true)) {
              return;
            }

            if (!retVal.isDone()) {
              if (t instanceof ReadTimeoutException) {
                // ReadTimeoutException is a shared singleton with a misleading (suppressed) stack trace. Report a
                // fresh instance instead. Note: we must use the no-arg constructor, since ReadTimeoutException(String)
                // (via ChannelException(String, Throwable, boolean)) trips an `assert shared` and throws AssertionError
                // when assertions are enabled (e.g. under surefire). See netty ChannelException.
                log.debug("[%s] Read timed out", requestDesc);
                retVal.setException(new ReadTimeoutException());
              } else {
                retVal.setException(t);
              }
            }

            // response is non-null if we received initial chunk and then exception occurs
            if (response != null) {
              handler.exceptionCaught(response, t);
            }
            try {
              if (closeIfNotOpen || channel.isOpen()) {
                channel.close();
              }
            }
            catch (Exception e) {
              log.warn(e, "[%s] Error while closing channel", requestDesc);
            }
            finally {
              channelResourceContainer.returnResource();
            }
          }

          private void removeHandlers()
          {
            if (readTimeout > 0) {
              channel.pipeline().remove(READ_TIMEOUT_HANDLER_NAME);
            }
            channel.pipeline().remove(LAST_HANDLER_NAME);
          }
        }
    );

    channel.writeAndFlush(httpRequest).addListener(
        new ChannelFutureListener()
        {
          @Override
          public void operationComplete(ChannelFuture future)
          {
            if (!future.isSuccess()) {
              channel.close();
              channelResourceContainer.returnResource();
              if (!retVal.isDone()) {
                retVal.setException(
                    new ChannelException(
                        StringUtils.format("[%s] Failed to write request to channel", requestDesc),
                        future.cause()
                    )
                );
              }
              // Note: Netty automatically releases the httpRequest after write attempt (success or failure)
            }
          }
        }
    );

    return retVal;
  }

  private long getReadTimeout(Duration requestReadTimeout)
  {
    final long timeout;
    if (requestReadTimeout != null) {
      timeout = requestReadTimeout.getMillis();
    } else if (defaultReadTimeout != null) {
      timeout = defaultReadTimeout.getMillis();
    } else {
      timeout = 0;
    }

    if (timeout > 0 && timer == null) {
      log.warn("Cannot time out requests without a timer! Disabling timeout for this request.");
      return 0;
    } else {
      return timeout;
    }
  }

  private String getHost(URL url)
  {
    int port = url.getPort();

    if (port == -1) {
      final String protocol = url.getProtocol();

      if ("http".equalsIgnoreCase(protocol)) {
        port = 80;
      } else if ("https".equalsIgnoreCase(protocol)) {
        port = 443;
      } else {
        throw new IAE("Cannot figure out default port for protocol[%s], please set Host header.", protocol);
      }
    }

    return url.getHost() + ":" + port;
  }

  private String getPoolKey(URL url)
  {
    return url.getProtocol() + "://" + url.getHost() + ":"
           + (url.getPort() == -1 ? url.getDefaultPort() : url.getPort());
  }

  /**
   * A read-timeout handler that fires a {@link ReadTimeoutException} down the pipeline if no inbound message is read
   * within the configured timeout. It behaves like Netty's {@link io.netty.handler.timeout.ReadTimeoutHandler} but
   * drives its timer off a shared {@link Timer} (a {@code HashedWheelTimer} dedicated thread) rather than the channel's
   * event loop.
   *
   * This avoids a class of problems where the event loop's blocking {@code epoll_wait}/{@code select} is interrupted by
   * signals (for example a profiler agent), which can reset the wait and cause event-loop-scheduled timeouts to be
   * delayed or never fire (see netty/netty#14368 and netty/netty#16244). The pre-Netty-4 Druid client scheduled read
   * timeouts on a {@code HashedWheelTimer} for the same reason.
   */
  static class TimerReadTimeoutHandler extends ChannelInboundHandlerAdapter
  {
    private final Timer timer;
    private final long timeoutNanos;

    private volatile long lastReadTimeNanos;
    private volatile Timeout scheduledTimeout;
    private volatile boolean destroyed;
    private boolean timedOut;

    TimerReadTimeoutHandler(Timer timer, long timeoutMillis)
    {
      this.timer = Preconditions.checkNotNull(timer, "timer");
      this.timeoutNanos = Math.max(TimeUnit.MILLISECONDS.toNanos(timeoutMillis), 1L);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx)
    {
      // The channel is typically already active (taken from the pool) by the time this handler is added.
      if (ctx.channel().isActive()) {
        initialize(ctx);
      }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx)
    {
      initialize(ctx);
      ctx.fireChannelActive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
      lastReadTimeNanos = System.nanoTime();
      ctx.fireChannelRead(msg);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx)
    {
      destroy();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
      destroy();
      ctx.fireChannelInactive();
    }

    private void initialize(ChannelHandlerContext ctx)
    {
      if (destroyed) {
        return;
      }
      lastReadTimeNanos = System.nanoTime();
      schedule(ctx, timeoutNanos);
    }

    private void schedule(final ChannelHandlerContext ctx, final long delayNanos)
    {
      if (destroyed) {
        return;
      }
      scheduledTimeout = timer.newTimeout(
          new TimerTask()
          {
            @Override
            public void run(Timeout t)
            {
              if (t.isCancelled() || destroyed || !ctx.channel().isOpen()) {
                return;
              }

              final long nextDelayNanos = timeoutNanos - (System.nanoTime() - lastReadTimeNanos);
              if (nextDelayNanos <= 0) {
                // Fire the timeout on the event loop, since pipeline events must run there.
                ctx.executor().execute(() -> {
                  if (timedOut || destroyed || !ctx.channel().isOpen()) {
                    return;
                  }
                  timedOut = true;
                  ctx.fireExceptionCaught(ReadTimeoutException.INSTANCE);
                });
              } else {
                // A read happened since we scheduled: reschedule for the remaining time.
                schedule(ctx, nextDelayNanos);
              }
            }
          },
          delayNanos,
          TimeUnit.NANOSECONDS
      );
    }

    private void destroy()
    {
      destroyed = true;
      final Timeout t = scheduledTimeout;
      if (t != null) {
        t.cancel();
        scheduledTimeout = null;
      }
    }
  }

}
