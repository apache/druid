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
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.ReferenceCountUtil;
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

/**
 */
public class NettyHttpClient extends AbstractHttpClient
{
  private static final Logger log = new Logger(NettyHttpClient.class);

  private static final String READ_TIMEOUT_HANDLER_NAME = "read-timeout";
  private static final String LAST_HANDLER_NAME = "last-handler";

  private final ResourcePool<String, ChannelFuture> pool;
  private final HttpClientConfig.CompressionCodec compressionCodec;
  private final Duration defaultReadTimeout;
  private long backPressureStartTimeNs;

  NettyHttpClient(
      ResourcePool<String, ChannelFuture> pool,
      Duration defaultReadTimeout,
      HttpClientConfig.CompressionCodec compressionCodec
  )
  {
    this.pool = Preconditions.checkNotNull(pool, "pool");
    this.defaultReadTimeout = defaultReadTimeout;
    this.compressionCodec = Preconditions.checkNotNull(compressionCodec);
  }

  @LifecycleStart
  public void start()
  {
  }

  @LifecycleStop
  public void stop()
  {
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
    }
    final String urlFile = StringUtils.nullToEmptyNonDruidDataString(url.getFile());
    final HttpRequest httpRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        method,
        urlFile.isEmpty() ? "/" : urlFile,
        request.hasContent() ? request.getContent() : Unpooled.EMPTY_BUFFER
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

    if (readTimeout > 0) {
      channel.pipeline().addLast(
          READ_TIMEOUT_HANDLER_NAME,
          new ReadTimeoutHandler(readTimeout, TimeUnit.MILLISECONDS)
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
            try {
              if (log.isDebugEnabled()) {
                log.debug("[%s] messageReceived: %s", requestDesc, msg);
              }
              if (msg instanceof HttpObject) {
                HttpObject httpObject = (HttpObject) msg;
                final DecoderResult decoderResult = httpObject.decoderResult();
                if (decoderResult.isFailure()) {
                  sendError(decoderResult.cause());
                  return;
                }
              }

              if (msg instanceof HttpResponse) {
                HttpResponse httpResponse = (HttpResponse) msg;
                if (log.isDebugEnabled()) {
                  log.debug("[%s] Got response: %s", requestDesc, httpResponse.status());
                }

                HttpResponseHandler.TrafficCop trafficCop = resumeChunkNum -> {
                  synchronized (watermarkLock) {
                    resumeWatermark = Math.max(resumeWatermark, resumeChunkNum);

                    if (suspendWatermark >= 0 && resumeWatermark >= suspendWatermark) {
                      suspendWatermark = -1;
                      long backPressureDuration = System.nanoTime() - backPressureStartTimeNs;
                      log.debug("[%s] Resumed reads from channel (chunkNum = %,d).", requestDesc, resumeChunkNum);
                      channel.read();
                      return backPressureDuration;
                    }
                  }

                  return 0; //If we didn't resume, don't know if backpressure was happening
                };
                response = handler.handleResponse(httpResponse, trafficCop);
                if (response.isFinished()) {
                  retVal.set((Final) response.getObj());
                }

                assert currentChunkNum == 0;
                possiblyRead(response);
              } else if (msg instanceof HttpContent) {
                HttpContent httpChunk = (HttpContent) msg;
                if (log.isDebugEnabled()) {
                  log.debug(
                      "[%s] Got chunk: %sB, last=%s",
                      requestDesc,
                      httpChunk.content().readableBytes(),
                      httpChunk instanceof LastHttpContent
                  );
                }

                response = handler.handleChunk(response, httpChunk, ++currentChunkNum);
                if (response.isFinished() && !retVal.isDone()) {
                  retVal.set((Final) response.getObj());
                }

                if (httpChunk instanceof LastHttpContent) {
                  finishRequest();
                } else {
                  possiblyRead(response);
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
              ReferenceCountUtil.release(msg);
            }
          }

          private void possiblyRead(ClientResponse<?> response)
          {
            if (response.isContinueReading()) {
              channel.read();
            } else {
              synchronized (watermarkLock) {
                suspendWatermark = Math.max(suspendWatermark, currentChunkNum);
                if (suspendWatermark > resumeWatermark) {
                  backPressureStartTimeNs = System.nanoTime();
                  log.debug("[%s] Delaying reads from channel (chunkNum = %,d).", requestDesc, currentChunkNum);
                } else {
                  channel.read();
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
            channelResourceContainer.returnResource();
          }

          @Override
          public void exceptionCaught(ChannelHandlerContext context, Throwable cause)
          {
            if (log.isDebugEnabled()) {
              if (cause == null) {
                log.debug("[%s] Caught exception", requestDesc);
              } else {
                log.debug(cause, "[%s] Caught exception", requestDesc);
              }
            }

            sendError(cause);
          }

          private void sendError(Throwable cause)
          {
            retVal.setException(cause);
            // response is non-null if we received initial chunk and then exception occurs
            if (response != null) {
              handler.exceptionCaught(response, cause);
            }
            try {
              if (channel.isOpen()) {
                channel.close();
              }
            }
            catch (Exception e) {
              log.warn(e, "Error while closing channel");
            }
            finally {
              channelResourceContainer.returnResource();
            }
          }

          @Override
          public void channelInactive(ChannelHandlerContext context)
          {
            if (log.isDebugEnabled()) {
              log.debug("[%s] Channel disconnected", requestDesc);
            }
            // response is non-null if we received initial chunk and then exception occurs
            if (response != null && response.isContinueReading()) {
              handler.exceptionCaught(response, new ChannelException("Channel disconnected"));
            }
            channel.close();
            channelResourceContainer.returnResource();
            if (!retVal.isDone()) {
              log.warn("[%s] Channel disconnected before response complete", requestDesc);
              retVal.setException(new ChannelException("Channel disconnected"));
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
        (ChannelFutureListener) future -> {
          if (future.isSuccess()) {
            channel.read();
          } else {
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

    return timeout;
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
}
