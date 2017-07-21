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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.InputStreamResponseHandler;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.server.DruidNode;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.initialization.jetty.ServletFilterHolder;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Server;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class JettyTest extends BaseJettyTest
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

                Multibinder<ServletFilterHolder> multibinder = Multibinder.newSetBinder(
                    binder,
                    ServletFilterHolder.class
                );

                multibinder.addBinding().toInstance(
                    new ServletFilterHolder()
                    {

                      @Override
                      public String getPath()
                      {
                        return "/*";
                      }

                      @Override
                      public Map<String, String> getInitParameters()
                      {
                        return null;
                      }

                      @Override
                      public Class<? extends Filter> getFilterClass()
                      {
                        return DummyAuthFilter.class;
                      }

                      @Override
                      public Filter getFilter()
                      {
                        return null;
                      }

                      @Override
                      public EnumSet<DispatcherType> getDispatcherType()
                      {
                        return null;
                      }
                    });

                Jerseys.addResource(binder, SlowResource.class);
                Jerseys.addResource(binder, ExceptionResource.class);
                Jerseys.addResource(binder, DefaultResource.class);
                LifecycleModule.register(binder, Server.class);
              }
            }
        )
    );

  }

  @Test
  @Ignore // this test will deadlock if it hits an issue, so ignored by default
  public void testTimeouts() throws Exception
  {
    // test for request timeouts properly not locking up all threads
    final Executor executor = Executors.newFixedThreadPool(100);
    final AtomicLong count = new AtomicLong(0);
    final CountDownLatch latch = new CountDownLatch(1000);
    for (int i = 0; i < 10000; i++) {
      executor.execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              executor.execute(
                  new Runnable()
                  {
                    @Override
                    public void run()
                    {
                      long startTime = System.currentTimeMillis();
                      long startTime2 = 0;
                      try {
                        ListenableFuture<StatusResponseHolder> go =
                            client.go(
                                new Request(HttpMethod.GET, new URL("http://localhost:" + port + "/slow/hello")),
                                new StatusResponseHandler(Charset.defaultCharset())
                            );
                        startTime2 = System.currentTimeMillis();
                        go.get();
                      }
                      catch (Exception e) {
                        e.printStackTrace();
                      }
                      finally {
                        System.out
                            .println(
                                "Response time client"
                                + (System.currentTimeMillis() - startTime)
                                + "time taken for getting future"
                                + (System.currentTimeMillis() - startTime2)
                                + "Counter " + count.incrementAndGet()
                            );
                        latch.countDown();

                      }
                    }
                  }
              );
            }
          }
      );
    }

    latch.await();
  }

  @Test
  public void testGzipCompression() throws Exception
  {
    final URL url = new URL("http://localhost:" + port + "/default");
    final HttpURLConnection get = (HttpURLConnection) url.openConnection();
    get.setRequestProperty("Accept-Encoding", "gzip");
    Assert.assertEquals("gzip", get.getContentEncoding());

    final HttpURLConnection post = (HttpURLConnection) url.openConnection();
    post.setRequestProperty("Accept-Encoding", "gzip");
    post.setRequestMethod("POST");
    Assert.assertEquals("gzip", post.getContentEncoding());

    final HttpURLConnection getNoGzip = (HttpURLConnection) url.openConnection();
    Assert.assertNotEquals("gzip", getNoGzip.getContentEncoding());

    final HttpURLConnection postNoGzip = (HttpURLConnection) url.openConnection();
    postNoGzip.setRequestMethod("POST");
    Assert.assertNotEquals("gzip", postNoGzip.getContentEncoding());
  }

  // Tests that threads are not stuck when partial chunk is not finalized
  // https://bugs.eclipse.org/bugs/show_bug.cgi?id=424107
  @Test
  @Ignore
  // above bug is not fixed in jetty for gzip encoding, and the chunk is still finalized instead of throwing exception.
  public void testChunkNotFinalized() throws Exception
  {
    ListenableFuture<InputStream> go =
        client.go(
            new Request(HttpMethod.GET, new URL("http://localhost:" + port + "/exception/exception")),
            new InputStreamResponseHandler()
        );
    try {
      StringWriter writer = new StringWriter();
      IOUtils.copy(go.get(), writer, "utf-8");
      Assert.fail("Should have thrown Exception");
    }
    catch (IOException e) {
      // Expected.
    }

  }

  @Test
  public void testThreadNotStuckOnException() throws Exception
  {
    final CountDownLatch latch = new CountDownLatch(1);
    Executors.newSingleThreadExecutor().execute(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              ListenableFuture<InputStream> go = client.go(
                  new Request(HttpMethod.GET, new URL("http://localhost:" + port + "/exception/exception")),
                  new InputStreamResponseHandler()
              );
              StringWriter writer = new StringWriter();
              IOUtils.copy(go.get(), writer, "utf-8");
            }
            catch (IOException e) {
              // Expected.
            }
            catch (Throwable t) {
              Throwables.propagate(t);
            }
            latch.countDown();
          }
        }
    );

    latch.await(5, TimeUnit.SECONDS);
  }

  @Test
  public void testExtensionAuthFilter() throws Exception
  {
    URL url = new URL("http://localhost:" + port + "/default");
    HttpURLConnection get = (HttpURLConnection) url.openConnection();
    get.setRequestProperty(DummyAuthFilter.AUTH_HDR, DummyAuthFilter.SECRET_USER);
    Assert.assertEquals(HttpServletResponse.SC_OK, get.getResponseCode());

    get = (HttpURLConnection) url.openConnection();
    get.setRequestProperty(DummyAuthFilter.AUTH_HDR, "hacker");
    Assert.assertEquals(HttpServletResponse.SC_UNAUTHORIZED, get.getResponseCode());
  }
}
