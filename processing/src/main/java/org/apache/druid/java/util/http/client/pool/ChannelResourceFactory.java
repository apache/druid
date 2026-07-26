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

package org.apache.druid.java.util.http.client.pool;

import com.google.common.base.Preconditions;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClientProxyConfig;
import org.apache.druid.java.util.http.client.Request;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ChannelResourceFactory implements ResourceFactory<String, ChannelFuture>
{
  private static final Logger log = new Logger(ChannelResourceFactory.class);

  private static final long DEFAULT_SSL_HANDSHAKE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);
  private static final long PROXY_HANDSHAKE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);
  private static final String PROXY_HANDLER_NAME = "druid-proxy";
  private static final String ERROR_HANDLER_NAME = "druid-connection-error";

  private final Bootstrap bootstrap;
  private final SSLContext sslContext;
  private final HttpClientProxyConfig proxyConfig;
  private final Timer timer;
  private final long sslHandshakeTimeout;

  public ChannelResourceFactory(
      Bootstrap bootstrap,
      SSLContext sslContext,
      HttpClientProxyConfig proxyConfig,
      Timer timer,
      long sslHandshakeTimeout
  )
  {
    this.bootstrap = Preconditions.checkNotNull(bootstrap, "bootstrap");
    this.sslContext = sslContext;
    this.proxyConfig = proxyConfig;
    this.timer = timer;
    this.sslHandshakeTimeout = sslHandshakeTimeout >= 0 ? sslHandshakeTimeout : DEFAULT_SSL_HANDSHAKE_TIMEOUT_MILLIS;

    if (sslContext != null) {
      Preconditions.checkNotNull(timer, "timer is required when sslContext is present");
    }
  }

  @Override
  public ChannelFuture generate(final String hostname)
  {
    log.debug("Generating: %s", hostname);
    URL url;
    try {
      url = new URL(hostname);
    }
    catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    final String host = url.getHost();
    final int port = url.getPort() == -1 ? url.getDefaultPort() : url.getPort();
    final ChannelFuture retVal;
    final ChannelFuture connectFuture;

    if (proxyConfig != null) {
      final ChannelFuture proxyFuture = bootstrap.connect(
          new InetSocketAddress(proxyConfig.getHost(), proxyConfig.getPort())
      );
      final ChannelPromise proxyConnectPromise = proxyFuture.channel().newPromise();
      connectFuture = proxyConnectPromise;

      final String proxyUri = StringUtils.format("%s:%d", host, port);
      DefaultFullHttpRequest connectRequest = new DefaultFullHttpRequest(
          HttpVersion.HTTP_1_1,
          HttpMethod.CONNECT,
          proxyUri
      );

      // The CONNECT exchange below is an ordinary application-level request/response, so nothing in
      // Netty bounds how long it may take. A proxy that accepts the TCP connection but never sends a
      // reply would leave proxyConnectPromise uncompleted forever, and callers block on it
      // uninterruptibly in NettyHttpClient#go, so the hang would be permanent. Bound it explicitly,
      // mirroring the SSL handshake timeout applied further down. The deadline is tracked on the
      // shared HashedWheelTimer rather than the channel's event loop so that it still fires if that
      // event loop is itself stalled.
      if (timer != null) {
        final Timeout connectTimeout = timer.newTimeout(
            t -> {
              final boolean failed = proxyConnectPromise.tryFailure(
                  new ChannelException(
                      StringUtils.format(
                          "Timed out after [%,d] ms waiting for a CONNECT response from proxy[%s]",
                          PROXY_HANDSHAKE_TIMEOUT_MILLIS,
                          proxyUri
                      )
                  )
              );
              if (failed) {
                proxyFuture.channel().close();
              }
            },
            PROXY_HANDSHAKE_TIMEOUT_MILLIS,
            TimeUnit.MILLISECONDS
        );
        proxyConnectPromise.addListener((ChannelFutureListener) f -> connectTimeout.cancel());
      }

      if (proxyConfig.getUser() != null) {
        connectRequest.headers().add(
            HttpHeaderNames.PROXY_AUTHORIZATION,
            Request.makeBasicAuthenticationString(proxyConfig.getUser(), proxyConfig.getPassword())
        );
      }

      proxyFuture.addListener(new ChannelFutureListener()
      {
        @Override
        public void operationComplete(ChannelFuture f1)
        {
          if (f1.isSuccess()) {
            final Channel channel = f1.channel();
            channel.pipeline().addLast(
                PROXY_HANDLER_NAME,
                new ChannelInboundHandlerAdapter()
                {
                  @Override
                  public void channelRead(ChannelHandlerContext ctx, Object msg)
                  {
                    final ChannelPipeline pipeline = ctx.pipeline();
                    pipeline.remove(PROXY_HANDLER_NAME);

                    if (msg instanceof HttpResponse) {
                      HttpResponse httpResponse = (HttpResponse) msg;
                      if (HttpResponseStatus.OK.equals(httpResponse.status())) {
                        // When the HttpClientCodec sees the CONNECT response complete, it goes into a "done"
                        // mode which makes it just do nothing.  Swap it with a new instance that will cover
                        // subsequent requests
                        pipeline.replace("codec", "codec", new HttpClientCodec());
                        proxyConnectPromise.setSuccess();
                      } else {
                        proxyConnectPromise.setFailure(
                            new ChannelException(
                                StringUtils.format(
                                    "Got status[%s] from CONNECT request to proxy[%s]",
                                    httpResponse.status(),
                                    proxyUri
                                )
                            )
                        );
                      }
                    } else {
                      proxyConnectPromise.setFailure(new ChannelException(StringUtils.format(
                          "Got message of type[%s], don't know what to do.", msg.getClass()
                      )));
                    }
                  }
                }
            );
            channel.writeAndFlush(connectRequest).addListener(
                new ChannelFutureListener()
                {
                  @Override
                  public void operationComplete(ChannelFuture f2)
                  {
                    if (!f2.isSuccess()) {
                      proxyConnectPromise.setFailure(
                          new ChannelException(
                              StringUtils.format("Problem with CONNECT request to proxy[%s]", proxyUri), f2.cause()
                          )
                      );
                    }
                  }
                }
            );
          } else {
            proxyConnectPromise.setFailure(
                new ChannelException(
                    StringUtils.format("Problem connecting to proxy[%s]", proxyUri), f1.cause()
                )
            );
          }
        }
      });
    } else {
      connectFuture = bootstrap.connect(new InetSocketAddress(host, port));
    }

    if ("https".equals(url.getProtocol())) {
      if (sslContext == null) {
        throw new IllegalStateException("No sslContext set, cannot do https");
      }

      final SSLEngine sslEngine = sslContext.createSSLEngine(host, port);
      final SSLParameters sslParameters = new SSLParameters();
      sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
      sslEngine.setSSLParameters(sslParameters);
      sslEngine.setUseClientMode(true);
      final SslHandler sslHandler = new SslHandler(sslEngine);
      sslHandler.setHandshakeTimeoutMillis(sslHandshakeTimeout);

      // https://github.com/netty/netty/issues/160
      // Netty 4 SslHandler doesn't have setCloseOnSSLException anymore?
      // It seems it closes by default on exception.

      final ChannelPromise handshakePromise = connectFuture.channel().newPromise();
      connectFuture.channel().pipeline().addLast(ERROR_HANDLER_NAME, new ConnectionErrorHandler(handshakePromise));
      connectFuture.addListener(
          new ChannelFutureListener()
          {
            @Override
            public void operationComplete(ChannelFuture f)
            {
              if (f.isSuccess()) {
                final ChannelPipeline pipeline = f.channel().pipeline();
                pipeline.addFirst("ssl", sslHandler);
                sslHandler.handshakeFuture().addListener(
                    new GenericFutureListener<Future<Channel>>()
                    {
                      @Override
                      public void operationComplete(Future<Channel> f2)
                      {
                        if (f2.isSuccess()) {
                          handshakePromise.setSuccess();
                        } else {
                          handshakePromise.setFailure(
                              new ChannelException(
                                  StringUtils.format("Failed to handshake with host[%s]", hostname),
                                  f2.cause()
                              )
                          );
                        }
                      }
                    }
                );
              } else {
                handshakePromise.setFailure(
                    new ChannelException(
                        StringUtils.format("Failed to connect to host[%s]", hostname),
                        f.cause()
                    )
                );
              }
            }
          }
      );

      retVal = handshakePromise;
    } else {
      connectFuture.channel().pipeline().addLast(ERROR_HANDLER_NAME, new ConnectionErrorHandler(null));
      retVal = connectFuture;
    }

    return retVal;
  }

  @Override
  public boolean isGood(ChannelFuture resource)
  {
    Channel channel = resource.channel();

    boolean isSuccess = resource.isSuccess();
    boolean isActive = channel.isActive();
    boolean isOpen = channel.isOpen();

    if (log.isTraceEnabled()) {
      log.trace("isGood = isSuccess[%s] && isActive[%s] && isOpen[%s]", isSuccess, isActive, isOpen);
    }

    return isSuccess && isActive && isOpen;
  }

  @Override
  public void close(ChannelFuture resource)
  {
    log.trace("Closing");
    // Close channel asynchronously - don't wait
    // Waiting can block shutdown and interrupt other operations
    // Netty will handle the close asynchronously
    resource.channel().close();
  }

  /**
   * Handler that captures errors that occur while connecting. Typically superseded by other handlers after
   * a connection happens, in {@link org.apache.druid.java.util.http.client.NettyHttpClient}.
   *
   * It's important to have this for all channels, even if {@link #future} is null, because otherwise exceptions
   * that occur during connection land at {@link io.netty.handler.codec.http.HttpContentDecompressor} (the last
   * handler from {@link org.apache.druid.java.util.http.client.netty.HttpClientPipelineFactory}) and are dropped on
   * the floor along with a scary-looking warning like "EXCEPTION, please implement
   * io.netty.handler.codec.http.HttpContentDecompressor.exceptionCaught() for proper handling."
   */
  private static class ConnectionErrorHandler extends ChannelInboundHandlerAdapter
  {
    @Nullable
    private final ChannelPromise future;

    /**
     * Constructor.
     *
     * @param future future to attach errors to
     */
    public ConnectionErrorHandler(@Nullable ChannelPromise future)
    {
      this.future = future;
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
    {
      final Channel channel = ctx.channel();
      if (channel == null) {
        // For the case where this pipeline is not attached yet.
        if (future != null && !future.isDone()) {
          final ChannelException e2 =
              new ChannelException(StringUtils.format("Channel is null. The context name is [%s]", ctx.name()));
          e2.addSuppressed(cause);
          future.setFailure(e2);
        }
        return;
      }

      if (future != null && !future.isDone()) {
        future.setFailure(cause);
      }

      // Close the channel if this is the last handler. Otherwise, we expect that NettyHttpClient would have added
      // additional handlers to take care of the errors.
      //noinspection ObjectEquality
      if (channel.isOpen() && this == ctx.pipeline().last()) {
        channel.close();
      }

      ctx.fireExceptionCaught(cause);
    }
  }
}
