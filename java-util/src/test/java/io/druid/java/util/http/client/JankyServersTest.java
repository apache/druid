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

package io.druid.java.util.http.client;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.http.client.response.StatusResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Tests with a bunch of goofy not-actually-http servers.
 */
public class JankyServersTest
{
  static ExecutorService exec;
  static ServerSocket silentServerSocket;
  static ServerSocket echoServerSocket;
  static ServerSocket closingServerSocket;

  @BeforeClass
  public static void setUp() throws Exception
  {
    exec = Executors.newCachedThreadPool();

    silentServerSocket = new ServerSocket(0);
    echoServerSocket = new ServerSocket(0);
    closingServerSocket = new ServerSocket(0);

    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = silentServerSocket.accept();
                  InputStream in = clientSocket.getInputStream()
              ) {
                while (in.read() != -1) {
                }
              }
              catch (Exception e) {
                // Suppress
              }
            }
          }
        }
    );

    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = closingServerSocket.accept();
                  InputStream in = clientSocket.getInputStream()
              ) {
                in.read();
                clientSocket.close();
              }
              catch (Exception e) {
                // Suppress
              }
            }
          }
        }
    );

    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = echoServerSocket.accept();
                  OutputStream out = clientSocket.getOutputStream();
                  InputStream in = clientSocket.getInputStream()
              ) {
                int b;
                while ((b = in.read()) != -1) {
                  out.write(b);
                }
              }
              catch (Exception e) {
                // Suppress
              }
            }
          }
        }
    );
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    exec.shutdownNow();
    silentServerSocket.close();
    echoServerSocket.close();
    closingServerSocket.close();
  }

  @Test
  public void testHttpSilentServerWithGlobalTimeout() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withReadTimeout(new Duration(100)).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final ListenableFuture<StatusResponseHolder> future = client
          .go(
              new Request(HttpMethod.GET, new URL(StringUtils.format("http://localhost:%d/", silentServerSocket.getLocalPort()))),
              new StatusResponseHandler(Charsets.UTF_8)
          );

      Throwable e = null;
      try {
        future.get();
      }
      catch (ExecutionException e1) {
        e = e1.getCause();
      }

      Assert.assertTrue("ReadTimeoutException thrown by 'get'", e instanceof ReadTimeoutException);
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testHttpSilentServerWithRequestTimeout() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withReadTimeout(new Duration(86400L * 365)).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final ListenableFuture<StatusResponseHolder> future = client
          .go(
              new Request(HttpMethod.GET, new URL(StringUtils.format("http://localhost:%d/", silentServerSocket.getLocalPort()))),
              new StatusResponseHandler(Charsets.UTF_8),
              new Duration(100L)
          );

      Throwable e = null;
      try {
        future.get();
      }
      catch (ExecutionException e1) {
        e = e1.getCause();
      }

      Assert.assertTrue("ReadTimeoutException thrown by 'get'", e instanceof ReadTimeoutException);
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testHttpsSilentServer() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder()
                                                      .withSslContext(SSLContext.getDefault())
                                                      .withSslHandshakeTimeout(new Duration(100))
                                                      .build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);

      final ListenableFuture<StatusResponseHolder> response = client
          .go(
              new Request(HttpMethod.GET, new URL(StringUtils.format("https://localhost:%d/", silentServerSocket.getLocalPort()))),
              new StatusResponseHandler(Charsets.UTF_8)
          );

      Throwable e = null;
      try {
        response.get();
      }
      catch (ExecutionException e1) {
        e = e1.getCause();
      }

      Assert.assertTrue("ChannelException thrown by 'get'", e instanceof ChannelException);
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testHttpConnectionClosingServer() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final ListenableFuture<StatusResponseHolder> response = client
          .go(
              new Request(HttpMethod.GET, new URL(StringUtils.format("http://localhost:%d/", closingServerSocket.getLocalPort()))),
              new StatusResponseHandler(Charsets.UTF_8)
          );
      Throwable e = null;
      try {
        response.get();
      }
      catch (ExecutionException e1) {
        e = e1.getCause();
        e1.printStackTrace();
      }

      Assert.assertTrue("ChannelException thrown by 'get'", isChannelClosedException(e));
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testHttpsConnectionClosingServer() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withSslContext(SSLContext.getDefault()).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);

      final ListenableFuture<StatusResponseHolder> response = client
          .go(
              new Request(HttpMethod.GET, new URL(StringUtils.format("https://localhost:%d/", closingServerSocket.getLocalPort()))),
              new StatusResponseHandler(Charsets.UTF_8)
          );

      Throwable e = null;
      try {
        response.get();
      }
      catch (ExecutionException e1) {
        e = e1.getCause();
        e1.printStackTrace();
      }

      Assert.assertTrue("ChannelException thrown by 'get'", isChannelClosedException(e));
    }
    finally {
      lifecycle.stop();
    }
  }

  public boolean isChannelClosedException(Throwable e)
  {
    return e instanceof ChannelException ||
           (e instanceof IOException && e.getMessage().matches(".*Connection reset by peer.*"));
  }

  @Test
  public void testHttpEchoServer() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final ListenableFuture<StatusResponseHolder> response = client
          .go(
              new Request(HttpMethod.GET, new URL(StringUtils.format("http://localhost:%d/", echoServerSocket.getLocalPort()))),
              new StatusResponseHandler(Charsets.UTF_8)
          );

      Throwable e = null;
      try {
        response.get();
      }
      catch (ExecutionException e1) {
        e = e1.getCause();
      }

      Assert.assertTrue("IllegalArgumentException thrown by 'get'", e instanceof IllegalArgumentException);
      Assert.assertTrue("Expected error message", e.getMessage().matches(".*invalid version format:.*"));
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test
  public void testHttpsEchoServer() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withSslContext(SSLContext.getDefault()).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);

      final ListenableFuture<StatusResponseHolder> response = client
          .go(
              new Request(HttpMethod.GET, new URL(StringUtils.format("https://localhost:%d/", echoServerSocket.getLocalPort()))),
              new StatusResponseHandler(Charsets.UTF_8)
          );

      Throwable e = null;
      try {
        response.get();
      }
      catch (ExecutionException e1) {
        e = e1.getCause();
      }

      Assert.assertNotNull("ChannelException thrown by 'get'", e);
    }
    finally {
      lifecycle.stop();
    }
  }
}
