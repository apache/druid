/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.initialization;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
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
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class JettyTest
{
  private Lifecycle lifecycle;
  private HttpClient client;
  private int port = -1;

  public static void setProperties()
  {
    System.setProperty("druid.server.http.numThreads", "20");
    System.setProperty("druid.server.http.maxIdleTime", "PT1S");
    System.setProperty("druid.global.http.readTimeout", "PT1S");
  }

  @Before
  public void setup() throws Exception
  {
    setProperties();
    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test", "localhost", null)
                );
                binder.bind(JettyServerInitializer.class).to(JettyServerInit.class).in(LazySingleton.class);
                Jerseys.addResource(binder, SlowResource.class);
                Jerseys.addResource(binder, ExceptionResource.class);
                Jerseys.addResource(binder, DefaultResource.class);
                LifecycleModule.register(binder, Server.class);
              }
            }
        )
    );
    final DruidNode node = injector.getInstance(Key.get(DruidNode.class, Self.class));
    port = node.getPort();

    lifecycle = injector.getInstance(Lifecycle.class);
    lifecycle.start();
    ClientHolder holder = injector.getInstance(ClientHolder.class);
    client = holder.getClient();
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
                            client.get(new URL("http://localhost:" + port + "/slow/hello"))
                                  .go(new StatusResponseHandler(Charset.defaultCharset()));
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
  }

  // Tests that threads are not stuck when partial chunk is not finalized
  // https://bugs.eclipse.org/bugs/show_bug.cgi?id=424107
  @Test
  @Ignore
  // above bug is not fixed in jetty for gzip encoding, and the chunk is still finalized instead of throwing exception.
  public void testChunkNotFinalized() throws Exception
  {
    ListenableFuture<InputStream> go =
        client.get(new URL("http://localhost:" + port + "/exception/exception"))
              .go(new InputStreamResponseHandler());
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
              ListenableFuture<InputStream> go = client.get(
                  new URL(
                      "http://localhost:" + port + "/exception/exception"
                  )

              )
                                                       .go(new InputStreamResponseHandler());
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

  @After
  public void teardown()
  {
    lifecycle.stop();
  }

  public static class ClientHolder
  {
    HttpClient client;

    ClientHolder()
    {
      try {
        this.client = HttpClientInit.createClient(
            new HttpClientConfig(1, SSLContext.getDefault(), Duration.ZERO),
            new Lifecycle()
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    public HttpClient getClient()
    {
      return client;
    }
  }

  public static class JettyServerInit extends BaseJettyServerInitializer
  {

    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      root.addFilter(defaultGzipFilterHolder(), "/*", null);
      root.addFilter(GuiceFilter.class, "/*", null);

      final HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{root});
      server.setHandler(handlerList);
    }
  }

  @Path("/slow")
  public static class SlowResource
  {

    public static Random random = new Random();

    @GET
    @Path("/hello")
    @Produces(MediaType.APPLICATION_JSON)
    public Response hello()
    {
      try {
        TimeUnit.MILLISECONDS.sleep(100 + random.nextInt(2000));
      }
      catch (InterruptedException e) {
        //
      }
      return Response.ok("hello").build();
    }
  }

  @Path("/default")
  public static class DefaultResource
  {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response get()
    {
      return Response.ok("hello").build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response post()
    {
      return Response.ok("hello").build();
    }

  }

  @Path("/exception")
  public static class ExceptionResource
  {
    @GET
    @Path("/exception")
    @Produces(MediaType.APPLICATION_JSON)
    public Response exception(
        @Context HttpServletResponse resp
    ) throws IOException
    {
      final ServletOutputStream outputStream = resp.getOutputStream();
      outputStream.println("hello");
      outputStream.flush();
      try {
        TimeUnit.MILLISECONDS.sleep(200);
      }
      catch (InterruptedException e) {
        //
      }
      throw new IOException();
    }
  }
}
