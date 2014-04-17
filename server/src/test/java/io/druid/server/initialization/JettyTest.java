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

package io.druid;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.guice.Jerseys;
import io.druid.guice.LazySingleton;
import io.druid.guice.annotations.Global;
import io.druid.initialization.Initialization;
import io.druid.server.initialization.JettyServerInitializer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.GzipFilter;
import org.junit.Ignore;
import org.junit.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
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
  public static void setProperties()
  {
    System.setProperty("druid.host", "localhost:9999");
    System.setProperty("druid.port", "9999");
    System.setProperty("druid.server.http.numThreads", "20");
    System.setProperty("druid.service", "test");
    System.setProperty("druid.server.http.maxIdleTime", "PT1S");
    System.setProperty("druid.global.http.readTimeout", "PT1S");
  }

  @Test @Ignore // this test will deadlock if it hits an issue, so ignored by default
  public void testTimeouts() throws Exception
  {
    // test for request timeouts properly not locking up all threads

    setProperties();
    Injector injector = Initialization.makeInjectorWithModules(
        Initialization.makeStartupInjector(), Lists.<Object>newArrayList(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(JettyServerInitializer.class).to(JettyServerInit.class).in(LazySingleton.class);
            Jerseys.addResource(binder, SlowResource.class);

          }
        }
    )
    );
    Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
    // Jetty is Lazy Initialized do a getInstance
    injector.getInstance(Server.class);
    lifecycle.start();
    ClientHolder holder = injector.getInstance(ClientHolder.class);
    final HttpClient client = holder.getClient();
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
                        ListenableFuture<StatusResponseHolder> go = client.get(
                            new URL(
                                "http://localhost:9999/slow/hello"
                            )
                        )
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
    lifecycle.stop();

  }

  public static class ClientHolder
  {
    HttpClient client;

    @Inject
    ClientHolder(@Global HttpClient client)
    {
      this.client = client;
    }

    public HttpClient getClient()
    {
      return client;
    }
  }

  public static class JettyServerInit implements JettyServerInitializer
  {

    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      root.addFilter(GzipFilter.class, "/*", null);
      root.addFilter(GuiceFilter.class, "/*", null);

      final HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{root, new DefaultHandler()});
      server.setHandler(handlerList);
    }
  }

  @Path("/slow")
  public static class SlowResource
  {

    public static Random random = new Random();

    @GET
    @Path("/hello")
    @Produces("application/json")
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
}
