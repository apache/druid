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
import io.netty.util.HashedWheelTimer;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.netty.HttpClientPipelineFactory;
import org.apache.druid.java.util.http.client.pool.ChannelResourceFactory;
import org.apache.druid.java.util.http.client.pool.ResourcePool;
import org.apache.druid.java.util.http.client.pool.ResourcePoolConfig;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HttpClientInit
{
  private static final Logger log = new Logger(HttpClientInit.class);

  public static HttpClient createClient(HttpClientConfig config, Lifecycle lifecycle)
  {
    try {
      // We need to use the full constructor in order to set a ThreadNameDeterminer. The other parameters are taken
      // from the defaults in HashedWheelTimer's other constructors.
      // Netty 4 HashedWheelTimer doesn't have ThreadNameDeterminer in the same way, or it's not needed.
      // We'll use a standard constructor.
      final HashedWheelTimer timer = new HashedWheelTimer(
          new ThreadFactoryBuilder().setDaemon(true)
                                    .setNameFormat("HttpClient-Timer-%s")
                                    .build(),
          100,
          TimeUnit.MILLISECONDS,
          512
      );

      // Start timer immediately - don't wait for lifecycle.start()
      // This ensures timeouts work even if lifecycle isn't started
      // Critical for preventing hung connections in production
      timer.start();

      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start()
            {
              // Timer already started in constructor
            }

            @Override
            public void stop()
            {
              timer.stop();
            }
          }
      );

      // Netty 4 uses EventLoopGroup instead of NioClientBossPool/NioWorkerPool
      // We typically only need one EventLoopGroup for the client.
      final EventLoopGroup workerGroup = new NioEventLoopGroup(
          config.getWorkerPoolSize(),
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("HttpClient-Netty-Worker-%s")
              .build()
      );

      final Bootstrap bootstrap = new Bootstrap();
      bootstrap.group(workerGroup)
               .channel(NioSocketChannel.class)
               .option(ChannelOption.SO_KEEPALIVE, true)
               .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
               .handler(new HttpClientPipelineFactory());

      InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);

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
              // Shutdown the EventLoopGroup
              // IMPORTANT: Do NOT wait for termination - it can interrupt in-flight requests
              // This is a deliberate trade-off for production stability

              // Initiate graceful shutdown - EventLoop will terminate when idle
              workerGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);

              // Do NOT wait - daemon threads will cleanup
              // Waiting can cause:
              // 1. Interruption of active HTTP requests (compactions, lookups)
              // 2. Production hangs if channels are stuck
              // 3. Cascade failures across components

              // Thread accumulation is acceptable because:
              // - Threads are daemon (won't prevent JVM exit)
              // - Production services are long-lived (not constantly restarting)
              // - Better to leak threads than interrupt critical operations

              log.debug("EventLoopGroup shutdown initiated (async)");
            }
          }
      );

      return lifecycle.addMaybeStartManagedInstance(
          new NettyHttpClient(
              new ResourcePool<>(
                  new ChannelResourceFactory(
                      bootstrap,
                      config.getSslContext(),
                      config.getProxyConfig(),
                      timer,
                      config.getSslHandshakeTimeout() == null ? -1 : config.getSslHandshakeTimeout().getMillis()
                  ),
                  new ResourcePoolConfig(
                      config.getNumConnections(),
                      config.getUnusedConnectionTimeoutDuration().getMillis()
                  ),
                  config.isEagerInitialization()
              ),
              config.getReadTimeout(),
              config.getCompressionCodec(),
              timer
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
}
