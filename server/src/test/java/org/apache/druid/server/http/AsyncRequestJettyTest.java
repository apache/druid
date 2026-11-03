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

package org.apache.druid.server.http;

import com.google.common.util.concurrent.SettableFuture;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.common.utils.SocketUtil;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.http.AsyncRequest.AsyncResponse;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link AsyncRequest} against a real Jetty server.
 */
public class AsyncRequestJettyTest
{
  private static final Duration READ_TIMEOUT = Duration.standardSeconds(5);

  /**
   * The future completes and wins the race to respond, but writes its response only after the request has started
   * timing out. Jetty then rejects the writing thread's call to complete the request, so unless the timeout listener
   * completes it, the connection is never read from again and the next request sent on it hangs.
   */
  @Test
  public void test_connectionIsReusable_whenFutureRespondsWhileRequestIsTimingOut() throws Exception
  {
    final CountDownLatch futureWonRace = new CountDownLatch(1);
    final CountDownLatch timeoutStarted = new CountDownLatch(1);
    final CountDownLatch responseWritten = new CountDownLatch(1);

    final int port = SocketUtil.findOpenPort(0);
    final Server server = new Server(port);
    final ServletContextHandler handler = new ServletContextHandler();
    final ServletHolder holder = new ServletHolder(new HttpServlet()
    {
      @Override
      protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException
      {
        if (!"/race".equals(req.getPathInfo())) {
          resp.setStatus(HttpServletResponse.SC_OK);
          resp.getWriter().write("pong");
          return;
        }

        final SettableFuture<String> future = SettableFuture.create();
        AsyncRequest.create(new TimeoutHookingRequest(req, timeoutStarted, responseWritten), future, 100L)
                    .contentType(TestHelper.JSON_MAPPER, MediaType.APPLICATION_JSON)
                    .onTimeout(HttpServletResponse.SC_NO_CONTENT)
                    .onSuccess(
                        result -> {
                          futureWonRace.countDown();
                          // Write the response only once the request has started timing out
                          await(timeoutStarted);
                          return AsyncResponse.of(HttpServletResponse.SC_OK, result);
                        }
                    )
                    .onFailure(t -> AsyncResponse.error(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, t.getMessage()))
                    .start();

        // Complete the future from another thread, as ChangeRequestHistory does for long-polling sync requests
        new Thread(() -> {
          future.set("the-result");
          responseWritten.countDown();
        }).start();

        // Jetty schedules the timeout only after this method returns, so this ensures the future wins the race
        await(futureWonRace);
      }
    });
    holder.setAsyncSupported(true);
    handler.addServlet(holder, "/*");
    server.setHandler(handler);

    final Lifecycle lifecycle = new Lifecycle();
    try {
      server.start();
      lifecycle.start();

      // A single connection, so that the second request is sent on the same connection as the first
      final HttpClient client = HttpClientInit.createClient(
          HttpClientConfig.builder().withNumConnections(1).build(),
          lifecycle
      );

      final StatusResponseHolder first = client.go(
          new Request(HttpMethod.GET, URI.create(StringUtils.format("http://localhost:%d/race", port)).toURL()),
          StatusResponseHandler.getInstance(),
          READ_TIMEOUT
      ).get();
      Assertions.assertEquals(HttpResponseStatus.OK, first.getStatus());
      Assertions.assertEquals("\"the-result\"", first.getContent());

      final StatusResponseHolder second = client.go(
          new Request(HttpMethod.GET, URI.create(StringUtils.format("http://localhost:%d/ping", port)).toURL()),
          StatusResponseHandler.getInstance(),
          READ_TIMEOUT
      ).get();
      Assertions.assertEquals(HttpResponseStatus.OK, second.getStatus());
      Assertions.assertEquals("pong", second.getContent());
    }
    finally {
      lifecycle.stop();
      server.stop();
    }
  }

  /**
   * Waits for the latch. The bound is only a safety net, since the latch is released promptly whether or not the
   * timeout listener completes the request.
   */
  private static void await(CountDownLatch latch)
  {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Timed out waiting for latch");
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Request whose {@link AsyncContext} wraps every listener added to it, which is how the timeout listener registered
   * by {@link AsyncRequest} gets hooked. The servlet API offers no other way to run code around that listener.
   */
  private static class TimeoutHookingRequest extends HttpServletRequestWrapper
  {
    private final CountDownLatch timeoutStarted;
    private final CountDownLatch responseWritten;

    TimeoutHookingRequest(HttpServletRequest request, CountDownLatch timeoutStarted, CountDownLatch responseWritten)
    {
      super(request);
      this.timeoutStarted = timeoutStarted;
      this.responseWritten = responseWritten;
    }

    @Override
    public AsyncContext startAsync()
    {
      final AsyncContext delegate = super.startAsync();
      return (AsyncContext) Proxy.newProxyInstance(
          AsyncContext.class.getClassLoader(),
          new Class<?>[]{AsyncContext.class},
          (proxy, method, args) -> {
            if ("addListener".equals(method.getName()) && args.length == 1) {
              delegate.addListener(
                  new TimeoutHookingListener((AsyncListener) args[0], timeoutStarted, responseWritten)
              );
              return null;
            }
            try {
              return method.invoke(delegate, args);
            }
            catch (InvocationTargetException e) {
              throw e.getCause();
            }
          }
      );
    }
  }

  /**
   * Signals that the request has started timing out before running the wrapped listener, and waits for the response
   * to be written before returning control to Jetty. This forces the order of events seen in production: the
   * response is written while the request is timing out, before Jetty acts on the outcome of the timeout listeners.
   */
  private static class TimeoutHookingListener implements AsyncListener
  {
    private final AsyncListener delegate;
    private final CountDownLatch timeoutStarted;
    private final CountDownLatch responseWritten;

    TimeoutHookingListener(AsyncListener delegate, CountDownLatch timeoutStarted, CountDownLatch responseWritten)
    {
      this.delegate = delegate;
      this.timeoutStarted = timeoutStarted;
      this.responseWritten = responseWritten;
    }

    @Override
    public void onTimeout(AsyncEvent event) throws IOException
    {
      timeoutStarted.countDown();
      delegate.onTimeout(event);
      await(responseWritten);
    }

    @Override
    public void onComplete(AsyncEvent event) throws IOException
    {
      delegate.onComplete(event);
    }

    @Override
    public void onError(AsyncEvent event) throws IOException
    {
      delegate.onError(event);
    }

    @Override
    public void onStartAsync(AsyncEvent event) throws IOException
    {
      delegate.onStartAsync(event);
    }
  }
}
