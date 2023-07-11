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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClientProxyConfig;
import org.apache.druid.java.util.http.client.Request;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.util.Timer;

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

  private final ClientBootstrap bootstrap;
  private final SSLContext sslContext;
  private final HttpClientProxyConfig proxyConfig;
  private final Timer timer;
  private final long sslHandshakeTimeout;

  public ChannelResourceFactory(
      ClientBootstrap bootstrap,
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
      connectFuture = Channels.future(proxyFuture.getChannel());

      final String proxyUri = StringUtils.format("%s:%d", host, port);
      DefaultHttpRequest connectRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.CONNECT, proxyUri);

      if (proxyConfig.getUser() != null) {
        connectRequest.headers().add(
            "Proxy-Authorization", Request.makeBasicAuthenticationString(
                proxyConfig.getUser(), proxyConfig.getPassword()
            )
        );
      }

      proxyFuture.addListener(new ChannelFutureListener()
      {
        @Override
        public void operationComplete(ChannelFuture f1)
        {
          if (f1.isSuccess()) {
            final Channel channel = f1.getChannel();
            channel.getPipeline().addLast(
                DRUID_PROXY_HANDLER,
                new SimpleChannelUpstreamHandler()
                {
                  @Override
                  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
                  {
                    Object msg = e.getMessage();

                    final ChannelPipeline pipeline = ctx.getPipeline();
                    pipeline.remove(DRUID_PROXY_HANDLER);

                    if (msg instanceof HttpResponse) {
                      HttpResponse httpResponse = (HttpResponse) msg;
                      if (HttpResponseStatus.OK.equals(httpResponse.getStatus())) {
                        // When the HttpClientCodec sees the CONNECT response complete, it goes into a "done"
                        // mode which makes it just do nothing.  Swap it with a new instance that will cover
                        // subsequent requests
                        pipeline.replace("codec", "codec", new HttpClientCodec());
                        connectFuture.setSuccess();
                      } else {
                        connectFuture.setFailure(
                            new ChannelException(
                                StringUtils.format(
                                    "Got status[%s] from CONNECT request to proxy[%s]",
                                    httpResponse.getStatus(),
                                    proxyUri
                                )
                            )
                        );
                      }
                    } else {
                      connectFuture.setFailure(new ChannelException(StringUtils.format(
                          "Got message of type[%s], don't know what to do.", msg.getClass()
                      )));
                    }
                  }
                }
            );
            channel.write(connectRequest).addListener(
                new ChannelFutureListener()
                {
                  @Override
                  public void operationComplete(ChannelFuture f2)
                  {
                    if (!f2.isSuccess()) {
                      connectFuture.setFailure(
                          new ChannelException(
                              StringUtils.format("Problem with CONNECT request to proxy[%s]", proxyUri), f2.getCause()
                          )
                      );
                    }
                  }
                }
            );
          } else {
            connectFuture.setFailure(
                new ChannelException(
                    StringUtils.format("Problem connecting to proxy[%s]", proxyUri), f1.getCause()
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
      final SslHandler sslHandler = new SslHandler(
          sslEngine,
          SslHandler.getDefaultBufferPool(),
          false,
          timer,
          sslHandshakeTimeout
      );

      // https://github.com/netty/netty/issues/160
      sslHandler.setCloseOnSSLException(true);

      final ChannelFuture handshakeFuture = Channels.future(connectFuture.getChannel());
      connectFuture.getChannel().getPipeline().addLast(
          "connectionErrorHandler", new SimpleChannelUpstreamHandler()
          {
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            {
              final Channel channel = ctx.getChannel();
              if (channel == null) {
                // For the case where this pipeline is not attached yet.
                handshakeFuture.setFailure(new ChannelException(
                    StringUtils.format("Channel is null. The context name is [%s]", ctx.getName())
                ));
                return;
              }
              handshakeFuture.setFailure(e.getCause());
              if (channel.isOpen()) {
                channel.close();
              }
            }
          }
      );
      connectFuture.addListener(
          new ChannelFutureListener()
          {
            @Override
            public void operationComplete(ChannelFuture f)
            {
              if (f.isSuccess()) {
                final ChannelPipeline pipeline = f.getChannel().getPipeline();
                pipeline.addFirst("ssl", sslHandler);
                sslHandler.handshake().addListener(
                    new ChannelFutureListener()
                    {
                      @Override
                      public void operationComplete(ChannelFuture f2)
                      {
                        if (f2.isSuccess()) {
                          handshakeFuture.setSuccess();
                        } else {
                          handshakeFuture.setFailure(
                              new ChannelException(
                                  StringUtils.format("Failed to handshake with host[%s]", hostname),
                                  f2.getCause()
                              )
                          );
                        }
                      }
                    }
                );
              } else {
                handshakeFuture.setFailure(
                    new ChannelException(
                        StringUtils.format("Failed to connect to host[%s]", hostname),
                        f.getCause()
                    )
                );
              }
            }
          }
      );

      retVal = handshakeFuture;
    } else {
      retVal = connectFuture;
    }

    return retVal;
  }

  @Override
  public boolean isGood(ChannelFuture resource)
  {
    Channel channel = resource.awaitUninterruptibly().getChannel();

    boolean isSuccess = resource.isSuccess();
    boolean isConnected = channel.isConnected();
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
    resource.awaitUninterruptibly().getChannel().close();
  }
}
