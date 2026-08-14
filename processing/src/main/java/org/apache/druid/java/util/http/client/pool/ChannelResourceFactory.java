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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslHandler;
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
  private static final String PROXY_HANDLER_NAME = "druid-proxy";
  private static final String ERROR_HANDLER_NAME = "druid-connection-error";

  private final Bootstrap bootstrap;
  private final SSLContext sslContext;
  private final HttpClientProxyConfig proxyConfig;
  private final long sslHandshakeTimeout;

  public ChannelResourceFactory(
      Bootstrap bootstrap,
      SSLContext sslContext,
      HttpClientProxyConfig proxyConfig,
      long sslHandshakeTimeout
  )
  {
    this.bootstrap = Preconditions.checkNotNull(bootstrap, "bootstrap");
    this.sslContext = sslContext;
    this.proxyConfig = proxyConfig;
    this.sslHandshakeTimeout = sslHandshakeTimeout >= 0 ? sslHandshakeTimeout : DEFAULT_SSL_HANDSHAKE_TIMEOUT_MILLIS;
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
      final ChannelPromise overallConnectPromise = proxyFuture.channel().newPromise();
      connectFuture = overallConnectPromise;

      final String proxyUri = StringUtils.format("%s:%d", host, port);
      final FullHttpRequest connectRequest = new DefaultFullHttpRequest(
          HttpVersion.HTTP_1_1,
          HttpMethod.CONNECT,
          proxyUri
      );

      if (proxyConfig.getUser() != null) {
        connectRequest.headers().add(
            "Proxy-Authorization", Request.makeBasicAuthenticationString(
                proxyConfig.getUser(), proxyConfig.getPassword()
            )
        );
      }

      proxyFuture.addListener((ChannelFuture f1) -> {
        if (f1.isSuccess()) {
          final Channel channel = f1.channel();
          channel.pipeline().addLast(
              PROXY_HANDLER_NAME,
              new SimpleChannelInboundHandler<HttpObject>()
              {
                private HttpResponseStatus responseStatus;

                @Override
                protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg)
                {
                  if (msg instanceof HttpResponse) {
                    responseStatus = ((HttpResponse) msg).status();
                  }
                  if (msg instanceof LastHttpContent) {
                    final ChannelPipeline pipeline = ctx.pipeline();
                    pipeline.remove(PROXY_HANDLER_NAME);

                    if (HttpResponseStatus.OK.equals(responseStatus)) {
                      // When the HttpClientCodec sees the CONNECT response complete, it goes into a "done"
                      // mode which makes it just do nothing.  Swap it with a new instance that will cover
                      // subsequent requests
                      pipeline.replace("codec", "codec", new HttpClientCodec());
                      overallConnectPromise.setSuccess();
                    } else {
                      overallConnectPromise.setFailure(
                          new ChannelException(
                              StringUtils.format(
                                  "Got status[%s] from CONNECT request to proxy[%s]",
                                  responseStatus,
                                  proxyUri
                              )
                          )
                      );
                    }
                  }
                }
              }
          );
          channel.writeAndFlush(connectRequest).addListener((ChannelFuture f2) -> {
            if (!f2.isSuccess()) {
              overallConnectPromise.setFailure(
                  new ChannelException(
                      StringUtils.format("Problem with CONNECT request to proxy[%s]", proxyUri), f2.cause()
                  )
              );
            }
          });
        } else {
          overallConnectPromise.setFailure(
              new ChannelException(
                  StringUtils.format("Problem connecting to proxy[%s]", proxyUri), f1.cause()
              )
          );
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

      final ChannelPromise handshakePromise = connectFuture.channel().newPromise();
      connectFuture.channel().pipeline().addLast(ERROR_HANDLER_NAME, new ConnectionErrorHandler(handshakePromise));
      connectFuture.addListener((ChannelFuture f) -> {
        if (f.isSuccess()) {
          final ChannelPipeline pipeline = f.channel().pipeline();
          pipeline.addFirst("ssl", sslHandler);
          sslHandler.handshakeFuture().addListener(f2 -> {
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
          });
        } else {
          handshakePromise.setFailure(
              new ChannelException(
                  StringUtils.format("Failed to connect to host[%s]", hostname),
                  f.cause()
              )
          );
        }
      });

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
    Channel channel = resource.awaitUninterruptibly().channel();

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
    resource.awaitUninterruptibly().channel().close();
  }

  /**
   * Handler that captures errors that occur while connecting. Typically superseded by other handlers after
   * a connection happens, in {@link org.apache.druid.java.util.http.client.NettyHttpClient}.
   *
   * It's important to have this for all channels, even if {@link #promise} is null, because otherwise exceptions
   * that occur during connection land at {@link io.netty.handler.codec.http.HttpContentDecompressor} (the last
   * handler from {@link org.apache.druid.java.util.http.client.netty.HttpClientPipelineFactory}) and are dropped on
   * the floor along with a scary-looking warning.
   */
  private static class ConnectionErrorHandler extends ChannelInboundHandlerAdapter
  {
    @Nullable
    private final ChannelPromise promise;

    /**
     * Constructor.
     *
     * @param promise promise to attach errors to
     */
    public ConnectionErrorHandler(@Nullable ChannelPromise promise)
    {
      this.promise = promise;
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
    {
      final Channel channel = ctx.channel();
      if (channel == null) {
        // For the case where this pipeline is not attached yet.
        if (promise != null && !promise.isDone()) {
          final ChannelException e2 =
              new ChannelException(StringUtils.format("Channel is null. The context name is [%s]", ctx.name()));
          e2.addSuppressed(cause);
          promise.setFailure(e2);
        }
        return;
      }

      if (promise != null && !promise.isDone()) {
        promise.setFailure(cause);
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
