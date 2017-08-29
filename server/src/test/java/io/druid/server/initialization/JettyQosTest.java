/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.concurrent.Execs;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.server.DruidNode;
import io.druid.server.initialization.jetty.JettyBindings;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class JettyQosTest extends BaseJettyTest
{
  @Override
  protected Injector setupInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test", "localhost", null, null, new ServerConfig())
                );
                binder.bind(JettyServerInitializer.class).to(JettyServerInit.class).in(LazySingleton.class);
                Jerseys.addResource(binder, SlowResource.class);
                Jerseys.addResource(binder, ExceptionResource.class);
                Jerseys.addResource(binder, DefaultResource.class);
                JettyBindings.addQosFilter(binder, "/slow/*", 2);
                final ServerConfig serverConfig = new ObjectMapper().convertValue(
                    ImmutableMap.of("numThreads", "10"),
                    ServerConfig.class
                );
                binder.bind(ServerConfig.class).toInstance(serverConfig);
                LifecycleModule.register(binder, Server.class);
              }
            }
        )
    );
  }

  @Test
  public void testNumThreads()
  {
    // Just make sure the injector stuff for this test is actually working.
    Assert.assertEquals(
        10,
        ((QueuedThreadPool) server.getThreadPool()).getMaxThreads()
    );
  }

  @Test(timeout = 120_000L)
  public void testQoS() throws Exception
  {
    final int fastThreads = 20;
    final int slowThreads = 15;
    final int slowRequestsPerThread = 5;
    final int fastRequestsPerThread = 200;
    final HttpClient fastClient = new ClientHolder(fastThreads).getClient();
    final HttpClient slowClient = new ClientHolder(slowThreads).getClient();
    final ExecutorService fastPool = Execs.multiThreaded(fastThreads, "fast-%d");
    final ExecutorService slowPool = Execs.multiThreaded(slowThreads, "slow-%d");
    final CountDownLatch latch = new CountDownLatch(fastThreads * fastRequestsPerThread);
    final AtomicLong fastCount = new AtomicLong();
    final AtomicLong slowCount = new AtomicLong();
    final AtomicLong fastElapsed = new AtomicLong();
    final AtomicLong slowElapsed = new AtomicLong();

    for (int i = 0; i < slowThreads; i++) {
      slowPool.submit(new Runnable()
      {
        @Override
        public void run()
        {
          for (int i = 0; i < slowRequestsPerThread; i++) {
            long startTime = System.currentTimeMillis();
            try {
              ListenableFuture<StatusResponseHolder> go =
                  slowClient.go(
                      new Request(HttpMethod.GET, new URL("http://localhost:" + port + "/slow/hello")),
                      new StatusResponseHandler(Charset.defaultCharset())
                  );
              go.get();
              slowCount.incrementAndGet();
              slowElapsed.addAndGet(System.currentTimeMillis() - startTime);
            }
            catch (InterruptedException e) {
              // BE COOL
            }
            catch (Exception e) {
              e.printStackTrace();
              throw Throwables.propagate(e);
            }
          }
        }
      });
    }

    // wait for jetty server pool to completely fill up
    while (server.getThreadPool().getIdleThreads() != 0) {
      Thread.sleep(25);
    }

    for (int i = 0; i < fastThreads; i++) {
      fastPool.submit(new Runnable()
      {
        @Override
        public void run()
        {
          for (int i = 0; i < fastRequestsPerThread; i++) {
            long startTime = System.currentTimeMillis();
            try {
              ListenableFuture<StatusResponseHolder> go =
                  fastClient.go(
                      new Request(HttpMethod.GET, new URL("http://localhost:" + port + "/default")),
                      new StatusResponseHandler(Charset.defaultCharset())
                  );
              go.get();
              fastCount.incrementAndGet();
              fastElapsed.addAndGet(System.currentTimeMillis() - startTime);
              latch.countDown();
            }
            catch (InterruptedException e) {
              // BE COOL
            }
            catch (Exception e) {
              e.printStackTrace();
              throw Throwables.propagate(e);
            }
          }
        }
      });
    }

    // Wait for all fast requests to be served
    latch.await();

    slowPool.shutdownNow();
    fastPool.shutdown();

    // check that fast requests finished quickly
    Assert.assertTrue(fastElapsed.get() / fastCount.get() < 500);
  }
}
