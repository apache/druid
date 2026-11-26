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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.netty.HttpClientChannelInitializer;
import org.apache.druid.java.util.http.client.pool.ChannelResourceFactory;
import org.apache.druid.java.util.http.client.pool.ResourcePool;
import org.apache.druid.java.util.http.client.pool.ResourcePoolConfig;
import org.joda.time.Duration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HttpClientInit
{
  public static HttpClient createClient(HttpClientConfig config, Lifecycle lifecycle)
  {
    return createNettyClient(config, lifecycle);
  }

  public static NettyHttpClient createNettyClient(HttpClientConfig config, Lifecycle lifecycle)
  {
    try {
      return lifecycle.addMaybeStartManagedInstance(
          new NettyHttpClient(
              new ResourcePool<>(
                  new ChannelResourceFactory(
                      createBootstrap(
                          lifecycle,
                          config.getWorkerPoolSize(),
                          config.getConnectTimeout(),
                          config.getByteBufAllocator()
                      ),
                      config.getSslContext(),
                      config.getProxyConfig(),
                      config.getSslHandshakeTimeout() == null ? -1 : config.getSslHandshakeTimeout().getMillis()
                  ),
                  new ResourcePoolConfig(
                      config.getNumConnections(),
                      config.getUnusedConnectionTimeoutDuration().getMillis(),
                      config.getPoolImplementation()
                  ),
                  config.isEagerInitialization()
              ),
              config.getReadTimeout(),
              toAcceptEncoding(
                  config.getCompressionCodecs() == null
                  ? HttpClientConfig.DEFAULT_COMPRESSION_CODECS
                  : config.getCompressionCodecs()
              )
          )
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Builds an {@code Accept-Encoding} header value that lists the given codecs in order of preference. The first codec
   * has no quality value, which means {@code q=1}. Each later codec gets a lower quality value, so a server that honors
   * quality values picks the earliest codec it supports. For example, {@code [ZSTD, GZIP]} gives
   * {@code "zstd, gzip;q=0.5"}. An empty list gives {@code "identity"}.
   */
  public static String toAcceptEncoding(final List<HttpClientConfig.CompressionCodec> codecs)
  {
    if (codecs.isEmpty()) {
      return HttpClientConfig.CompressionCodec.IDENTITY.getEncodingString();
    }

    final StringBuilder builder = new StringBuilder(codecs.get(0).getEncodingString());
    final int numCodecs = codecs.size();
    for (int i = 1; i < numCodecs; i++) {
      // Quality values have at most three decimal places (RFC 9110, section 12.4.2).
      final String quality = StringUtils.format("%03d", (numCodecs - i) * 1000 / numCodecs);
      int length = quality.length();
      while (length > 1 && quality.charAt(length - 1) == '0') {
        length--;
      }
      builder.append(", ")
             .append(codecs.get(i).getEncodingString())
             .append(";q=0.")
             .append(quality, 0, length);
    }
    return builder.toString();
  }

  public static SSLContext sslContextWithTrustedKeyStore(final String keyStorePath, final String keyStorePassword)
  {
    try (FileInputStream in = new FileInputStream(keyStorePath)) {
      final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      ks.load(in, keyStorePassword.toCharArray());
      in.close();

      final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      final SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, tmf.getTrustManagers(), null);

      return sslContext;
    }
    catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
  }

  private static Bootstrap createBootstrap(
      Lifecycle lifecycle,
      int workerPoolSize,
      Duration connectTimeout,
      ByteBufAllocator byteBufAllocator
  )
  {
    final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(
        workerPoolSize,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("HttpClient-Netty-Worker-%s")
            .build()
    );

    final Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(eventLoopGroup)
             .channel(NioSocketChannel.class)
             .handler(new HttpClientChannelInitializer())
             .option(ChannelOption.SO_KEEPALIVE, true)
             .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(connectTimeout.getMillis()))
             .option(ChannelOption.ALLOCATOR, byteBufAllocator);

    InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);

    try {
      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start()
            {
            }

            @Override
            public void stop()
            {
              eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
            }
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    return bootstrap;
  }
}
