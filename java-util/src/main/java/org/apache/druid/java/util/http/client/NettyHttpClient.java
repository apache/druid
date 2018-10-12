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
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.Timer;
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

    final String requestDesc = StringUtils.format("%s %s", method, url);
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
              channelFuture.getCause()
          )
      );
    } else {
      channel = channelFuture.getChannel();

      // In case we get a channel that never had its readability turned back on.
      channel.setReadable(true);
    }
    final String urlFile = StringUtils.nullToEmptyNonDruidDataString(url.getFile());
    final HttpRequest httpRequest = new DefaultHttpRequest(
        HttpVersion.HTTP_1_1,
        method,
        urlFile.isEmpty() ? "/" : urlFile
    );

    if (!headers.containsKey(HttpHeaders.Names.HOST)) {
      httpRequest.headers().add(HttpHeaders.Names.HOST, getHost(url));
    }

    // If Accept-Encoding is set in the Request, use that. Otherwise use the default from "compressionCodec".
    if (!headers.containsKey(HttpHeaders.Names.ACCEPT_ENCODING)) {
      httpRequest.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, compressionCodec.getEncodingString());
    }

    for (Map.Entry<String, Collection<String>> entry : headers.asMap().entrySet()) {
      String key = entry.getKey();

      for (String obj : entry.getValue()) {
        httpRequest.headers().add(key, obj);
      }
    }

    if (request.hasContent()) {
      httpRequest.setContent(request.getContent());
    }

    final long readTimeout = getReadTimeout(requestReadTimeout);
    final SettableFuture<Final> retVal = SettableFuture.create();

    if (readTimeout > 0) {
      channel.getPipeline().addLast(
          READ_TIMEOUT_HANDLER_NAME,
          new ReadTimeoutHandler(timer, readTimeout, TimeUnit.MILLISECONDS)
      );
    }

    channel.getPipeline().addLast(
        LAST_HANDLER_NAME,
        new SimpleChannelUpstreamHandler()
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
          public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
          {
            if (log.isDebugEnabled()) {
              log.debug("[%s] messageReceived: %s", requestDesc, e.getMessage());
            }
            try {
              Object msg = e.getMessage();

              if (msg instanceof HttpResponse) {
                HttpResponse httpResponse = (HttpResponse) msg;
                if (log.isDebugEnabled()) {
                  log.debug("[%s] Got response: %s", requestDesc, httpResponse.getStatus());
                }

                HttpResponseHandler.TrafficCop trafficCop = resumeChunkNum -> {
                  synchronized (watermarkLock) {
                    resumeWatermark = Math.max(resumeWatermark, resumeChunkNum);

                    if (suspendWatermark >= 0 && resumeWatermark >= suspendWatermark) {
                      suspendWatermark = -1;
                      channel.setReadable(true);
                      long backPressureDuration = System.nanoTime() - backPressureStartTimeNs;
                      log.debug("[%s] Resumed reads from channel (chunkNum = %,d).", requestDesc, resumeChunkNum);
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
                possiblySuspendReads(response);

                if (!httpResponse.isChunked()) {
                  finishRequest();
                }
              } else if (msg instanceof HttpChunk) {
                HttpChunk httpChunk = (HttpChunk) msg;
                if (log.isDebugEnabled()) {
                  log.debug(
                      "[%s] Got chunk: %sB, last=%s",
                      requestDesc,
                      httpChunk.getContent().readableBytes(),
                      httpChunk.isLast()
                  );
                }

                if (httpChunk.isLast()) {
                  finishRequest();
                } else {
                  response = handler.handleChunk(response, httpChunk, ++currentChunkNum);
                  if (response.isFinished() && !retVal.isDone()) {
                    retVal.set((Final) response.getObj());
                  }
                  possiblySuspendReads(response);
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
          }

          private void possiblySuspendReads(ClientResponse<?> response)
          {
            if (!response.isContinueReading()) {
              synchronized (watermarkLock) {
                suspendWatermark = Math.max(suspendWatermark, currentChunkNum);
                if (suspendWatermark > resumeWatermark) {
                  channel.setReadable(false);
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
            channel.setReadable(true);
            channelResourceContainer.returnResource();
          }

          @Override
          public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event)
          {
            if (log.isDebugEnabled()) {
              final Throwable cause = event.getCause();
              if (cause == null) {
                log.debug("[%s] Caught exception", requestDesc);
              } else {
                log.debug(cause, "[%s] Caught exception", requestDesc);
              }
            }

            retVal.setException(event.getCause());
            // response is non-null if we received initial chunk and then exception occurs
            if (response != null) {
              handler.exceptionCaught(response, event.getCause());
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
          public void channelDisconnected(ChannelHandlerContext context, ChannelStateEvent event)
          {
            if (log.isDebugEnabled()) {
              log.debug("[%s] Channel disconnected", requestDesc);
            }
            // response is non-null if we received initial chunk and then exception occurs
            if (response != null) {
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
              channel.getPipeline().remove(READ_TIMEOUT_HANDLER_NAME);
            }
            channel.getPipeline().remove(LAST_HANDLER_NAME);
          }
        }
    );

    channel.write(httpRequest).addListener(
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
                        future.getCause()
                    )
                );
              }
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

    return StringUtils.format("%s:%s", url.getHost(), port);
  }

  private String getPoolKey(URL url)
  {
    return StringUtils.format(
        "%s://%s:%s", url.getProtocol(), url.getHost(), url.getPort() == -1 ? url.getDefaultPort() : url.getPort()
    );
  }
}
