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
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.netty.HttpClientInitializer;
import org.apache.druid.java.util.http.client.pool.ChannelResourceFactory;
import org.apache.druid.java.util.http.client.pool.ResourcePool;
import org.apache.druid.java.util.http.client.pool.ResourcePoolConfig;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.concurrent.Executors;

/**
 *
 */
public class HttpClientInit
{
  public static HttpClient createClient(HttpClientConfig config, Lifecycle lifecycle)
  {
    try {
      return lifecycle.addMaybeStartManagedInstance(
          new NettyHttpClient(
              new ResourcePool<>(
                  new ChannelResourceFactory(
                      createBootstrap(lifecycle, config.getWorkerPoolSize()),
                      config.getSslContext(),
                      config.getProxyConfig(),
                      config.getSslHandshakeTimeout() == null ? -1 : config.getSslHandshakeTimeout().getMillis()
                  ),
                  new ResourcePoolConfig(
                      config.getNumConnections(),
                      config.getUnusedConnectionTimeoutDuration().getMillis()
                  ),
                  config.isEagerInitialization()
              ),
              config.getReadTimeout(),
              config.getCompressionCodec()
          )
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
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

  private static Bootstrap createBootstrap(Lifecycle lifecycle, int workerPoolSize)
  {

    final EventLoopGroup workerGroup = new NioEventLoopGroup(
        workerPoolSize,
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("HttpClient-Netty-Worker-%s")
                .build()
        )
    );

    final Bootstrap bootstrap = new Bootstrap()
        .group(workerGroup)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.AUTO_READ, false)
        .handler(new HttpClientInitializer());

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
              workerGroup.shutdownGracefully();
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
