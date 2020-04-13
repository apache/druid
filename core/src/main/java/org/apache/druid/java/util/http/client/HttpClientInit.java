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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.netty.HttpClientPipelineFactory;
import org.apache.druid.java.util.http.client.pool.ChannelResourceFactory;
import org.apache.druid.java.util.http.client.pool.ResourcePool;
import org.apache.druid.java.util.http.client.pool.ResourcePoolConfig;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.socket.nio.NioClientBossPool;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.Timer;
import org.joda.time.Duration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 */
public class HttpClientInit
{
  public static HttpClient createClient(HttpClientConfig config, Lifecycle lifecycle)
  {
    try {
      // We need to use the full constructor in order to set a ThreadNameDeterminer. The other parameters are taken
      // from the defaults in HashedWheelTimer's other constructors.
      final HashedWheelTimer timer = new HashedWheelTimer(
          new ThreadFactoryBuilder().setDaemon(true)
                                    .setNameFormat("HttpClient-Timer-%s")
                                    .build(),
          ThreadNameDeterminer.CURRENT,
          100,
          TimeUnit.MILLISECONDS,
          512
      );
      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start()
            {
              timer.start();
            }

            @Override
            public void stop()
            {
              timer.stop();
            }
          }
      );
      return lifecycle.addMaybeStartManagedInstance(
          new NettyHttpClient(
              new ResourcePool<>(
                  new ChannelResourceFactory(
                      createBootstrap(lifecycle, timer, config.getBossPoolSize(), config.getWorkerPoolSize()),
                      config.getSslContext(),
                      timer,
                      config.getSslHandshakeTimeout() == null ? -1 : config.getSslHandshakeTimeout().getMillis()
                  ),
                  new ResourcePoolConfig(
                      config.getNumConnections(),
                      config.getUnusedConnectionTimeoutDuration().getMillis()
                  )
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

  @Deprecated
  public static HttpClient createClient(ResourcePoolConfig config, final SSLContext sslContext, Lifecycle lifecycle)
  {
    return createClient(
        new HttpClientConfig(config.getMaxPerKey(), sslContext, Duration.ZERO),
        lifecycle
    );
  }

  @Deprecated // use createClient directly
  public static ClientBootstrap createBootstrap(Lifecycle lifecycle, Timer timer)
  {
    final HttpClientConfig defaultConfig = HttpClientConfig.builder().build();
    return createBootstrap(lifecycle, timer, defaultConfig.getBossPoolSize(), defaultConfig.getWorkerPoolSize());
  }

  @Deprecated // use createClient directly
  public static ClientBootstrap createBootstrap(Lifecycle lifecycle)
  {
    final Timer timer = new HashedWheelTimer(new ThreadFactoryBuilder().setDaemon(true).build());
    return createBootstrap(lifecycle, timer);
  }

  public static SSLContext sslContextWithTrustedKeyStore(final String keyStorePath, final String keyStorePassword)
  {
    FileInputStream in = null;
    try {
      final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());

      in = new FileInputStream(keyStorePath);
      ks.load(in, keyStorePassword.toCharArray());
      in.close();

      final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      final SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, tmf.getTrustManagers(), null);

      return sslContext;
    }
    catch (CertificateException e) {
      throw new RuntimeException(e);
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    catch (KeyStoreException e) {
      throw new RuntimeException(e);
    }
    catch (KeyManagementException e) {
      throw new RuntimeException(e);
    }
    catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    finally {
      CloseQuietly.close(in);
    }
  }

  private static ClientBootstrap createBootstrap(Lifecycle lifecycle, Timer timer, int bossPoolSize, int workerPoolSize)
  {
    final NioClientBossPool bossPool = new NioClientBossPool(
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("HttpClient-Netty-Boss-%s")
                .build()
        ),
        bossPoolSize,
        timer,
        ThreadNameDeterminer.CURRENT
    );

    final NioWorkerPool workerPool = new NioWorkerPool(
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("HttpClient-Netty-Worker-%s")
                .build()
        ),
        workerPoolSize,
        ThreadNameDeterminer.CURRENT
    );

    final ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(bossPool, workerPool));

    bootstrap.setOption("keepAlive", true);
    bootstrap.setPipelineFactory(new HttpClientPipelineFactory());

    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());

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
              bootstrap.releaseExternalResources();
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
