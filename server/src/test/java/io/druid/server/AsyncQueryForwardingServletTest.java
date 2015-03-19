/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.initialization.Initialization;
import io.druid.query.Query;
import io.druid.server.initialization.BaseJettyTest;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.log.RequestLogger;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.server.router.QueryHostFinder;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.runners.statements.FailOnTimeout;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class AsyncQueryForwardingServletTest extends BaseJettyTest
{

  public void dumpThreads()
  {
    final StringBuilder dump = new StringBuilder("Thread dump \n");
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
    for (ThreadInfo threadInfo : threadInfos) {
      dump.append('"');
      dump.append(threadInfo.getThreadName());
      dump.append("\" ");
      final Thread.State state = threadInfo.getThreadState();
      dump.append("\n   java.lang.Thread.State: ");
      dump.append(state);
      final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
      for (final StackTraceElement stackTraceElement : stackTraceElements) {
        dump.append("\n        at ");
        dump.append(stackTraceElement);
      }
      dump.append("\n\n");
    }
    System.out.println(dump.toString());
  }

  @Before
  public void setup() throws Exception
  {
    System.out.println("Starting setup");
    setProperties("PT120S");
    Injector injector = setupInjector();
    System.out.println("Injector setup done");

    final DruidNode node = injector.getInstance(Key.get(DruidNode.class, Self.class));
    port = node.getPort();

    lifecycle = injector.getInstance(Lifecycle.class);
    lifecycle.start();
    ClientHolder holder = injector.getInstance(ClientHolder.class);
    client = holder.getClient();
    System.out.println("setup done");
  }

  @Override
  protected Injector setupInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test", "localhost", 9991)
                );
                binder.bind(JettyServerInitializer.class).to(ProxyJettyServerInit.class).in(LazySingleton.class);
                Jerseys.addResource(binder, SlowResource.class);
                Jerseys.addResource(binder, ExceptionResource.class);
                Jerseys.addResource(binder, DefaultResource.class);
                LifecycleModule.register(binder, Server.class);
              }
            }
        )
    );
  }

  @Rule
  public TestRule timeout = new TestRule()
  {
    @Override
    public Statement apply(
        Statement base, Description description
    )
    {
      return new FailOnTimeout(base, TimeUnit.MINUTES.toMillis(5))
      {
        @Override
        public void evaluate() throws Throwable
        {
          try {
            super.evaluate();
          } catch(Exception e){
            dumpThreads();
            throw e;
          }
        }
      };
    }
  };

  @Test
  public void testProxyGzipCompression() throws Exception
  {
    System.out.println("testing testProxyGzipCompression");

    final URL url = new URL("http://localhost:" + port + "/proxy/default");
    System.out.println(url.toString());
    final HttpURLConnection get = (HttpURLConnection) url.openConnection();
    get.setRequestProperty("Accept-Encoding", "gzip");
    Assert.assertEquals("gzip", get.getContentEncoding());
    System.out.println("Assertion 1 Passed");

    final HttpURLConnection post = (HttpURLConnection) url.openConnection();
    post.setRequestProperty("Accept-Encoding", "gzip");
    post.setRequestMethod("POST");
    Assert.assertEquals("gzip", post.getContentEncoding());
    System.out.println("Assertion 2 Passed");

    final HttpURLConnection getNoGzip = (HttpURLConnection) url.openConnection();
    Assert.assertNotEquals("gzip", getNoGzip.getContentEncoding());
    System.out.println("Assertion 3 Passed");

    final HttpURLConnection postNoGzip = (HttpURLConnection) url.openConnection();
    postNoGzip.setRequestMethod("POST");
    Assert.assertNotEquals("gzip", postNoGzip.getContentEncoding());
    System.out.println("Assertion 4 Passed");

  }

  public static class ProxyJettyServerInit implements JettyServerInitializer
  {

    private final DruidNode node;

    @Inject
    public ProxyJettyServerInit(@Self DruidNode node)
    {
      this.node = node;
    }

    @Override
    public void initialize(Server server, Injector injector)
    {
      System.out.println("Initializing ProxyJettyServerInit");

      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

      final QueryHostFinder hostFinder = new QueryHostFinder(null)
      {
        @Override
        public String getHost(Query query)
        {
          return "localhost:" + node.getPort();
        }

        @Override
        public String getDefaultHost()
        {
          return "localhost:" + node.getPort();
        }
      };

      root.addServlet(
          new ServletHolder(
              new AsyncQueryForwardingServlet(
                  injector.getInstance(ObjectMapper.class),
                  injector.getInstance(Key.get(ObjectMapper.class, Smile.class)),
                  hostFinder,
                  injector.getProvider(org.eclipse.jetty.client.HttpClient.class),
                  injector.getInstance(DruidHttpClientConfig.class),
                  new NoopServiceEmitter(),
                  new RequestLogger()
                  {
                    @Override
                    public void log(RequestLogLine requestLogLine) throws IOException
                    {
                      // noop
                    }
                  }
              )
              {
                @Override
                protected URI rewriteURI(HttpServletRequest request)
                {
                  URI uri = super.rewriteURI(request);
                  return URI.create(uri.toString().replace("/proxy", ""));
                }
              }
          ), "/proxy/*"
      );
      JettyServerInitUtils.addExtensionFilters(root, injector);
      root.addFilter(JettyServerInitUtils.defaultAsyncGzipFilterHolder(), "/*", null);
      root.addFilter(GuiceFilter.class, "/slow/*", null);
      root.addFilter(GuiceFilter.class, "/default/*", null);
      root.addFilter(GuiceFilter.class, "/exception/*", null);

      final HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{root});
      server.setHandler(handlerList);
      System.out.println("Initializing ProxyJettyServerInit done");

    }
  }
}
