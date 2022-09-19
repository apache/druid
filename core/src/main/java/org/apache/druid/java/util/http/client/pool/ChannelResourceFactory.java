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
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClientProxyConfig;
import org.apache.druid.java.util.http.client.Request;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 */
public class ChannelResourceFactory implements ResourceFactory<String, ChannelFuture>
{
  private static final Logger log = new Logger(ChannelResourceFactory.class);

  private static final long DEFAULT_SSL_HANDSHAKE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);
  private static final String DRUID_PROXY_HANDLER = "druid_proxyHandler";

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
      ChannelPromise connectPromise = proxyFuture.channel().newPromise();
      connectFuture = connectPromise;

      final String proxyUri = host + ":" + port;
      DefaultHttpRequest connectRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.CONNECT, proxyUri);

      if (proxyConfig.getUser() != null) {
        connectRequest.headers().add(
            HttpHeaderNames.PROXY_AUTHORIZATION.toString(),
            Request.makeBasicAuthenticationString(
                proxyConfig.getUser(), proxyConfig.getPassword()
            )
        );
      }

      proxyFuture.addListener((ChannelFutureListener) f1 -> {
        if (f1.isSuccess()) {
          final Channel channel = f1.channel();
          channel.pipeline().addLast(
              DRUID_PROXY_HANDLER,
              new ChannelInboundHandlerAdapter()
              {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg)
                {
                  final ChannelPipeline pipeline = ctx.pipeline();
                  pipeline.remove(DRUID_PROXY_HANDLER);

                  if (msg instanceof HttpResponse) {
                    HttpResponse httpResponse = (HttpResponse) msg;
                    if (HttpResponseStatus.OK.equals(httpResponse.status())) {
                      connectPromise.setSuccess();
                    } else {
                      connectPromise.setFailure(
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
                    connectPromise.setFailure(new ChannelException(StringUtils.format(
                        "Got message of type[%s], don't know what to do.", msg.getClass()
                    )));
                  }
                  ReferenceCountUtil.release(msg);
                }

                @Override
                public void channelReadComplete(ChannelHandlerContext ctx)
                {
                  ctx.channel().read();
                }
              }
          );
          channel.write(connectRequest);
          channel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).addListener(
              (ChannelFutureListener) f2 -> {
                if (f2.isSuccess()) {
                  channel.read();
                } else {
                  connectPromise.setFailure(
                      new ChannelException(
                          StringUtils.format("Problem with CONNECT request to proxy[%s]", proxyUri), f2.cause()
                      )
                  );
                }
              }
          );
        } else {
          connectPromise.setFailure(
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
      connectFuture.channel().pipeline().addLast(
          "connectionErrorHandler", new ChannelInboundHandlerAdapter()
          {
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            {
              final Channel channel = ctx.channel();
              if (channel == null) {
                // For the case where this pipeline is not attached yet.
                handshakePromise.setFailure(new ChannelException(
                    StringUtils.format("Channel is null. The context name is [%s]", ctx.name())
                ));
                return;
              }
              handshakePromise.setFailure(cause);
              if (channel.isOpen()) {
                channel.close();
              }
            }
          }
      );
      connectFuture.addListener(
          (ChannelFutureListener) f -> {
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
      );

      retVal = handshakePromise;
    } else {
      retVal = connectFuture;
    }

    return retVal;
  }

  @Override
  public boolean isGood(ChannelFuture resource)
  {
    Channel channel = resource.awaitUninterruptibly().channel();

    boolean isSuccess = resource.isSuccess();
    boolean isConnected = channel.isActive();
    boolean isOpen = channel.isOpen();

    if (log.isTraceEnabled()) {
      log.trace("isGood = isSucess[%s] && isConnected[%s] && isOpen[%s]", isSuccess, isConnected, isOpen);
    }

    return isSuccess && isConnected && isOpen;
  }

  @Override
  public void close(ChannelFuture resource)
  {
    log.trace("Closing");
    resource.awaitUninterruptibly().channel().close();
  }
}
