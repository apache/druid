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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests with servers that are at least moderately well-behaving.
 */
public class FriendlyServersTest
{
  @Test
  public void testFriendlyHttpServer() throws Exception
  {
    final ExecutorService exec = Executors.newSingleThreadExecutor();
    final ServerSocket serverSocket = new ServerSocket(0);
    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = serverSocket.accept();
                  BufferedReader in = new BufferedReader(
                      new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8)
                  );
                  OutputStream out = clientSocket.getOutputStream()
              ) {
                while (!in.readLine().equals("")) {
                  // skip lines
                }
                out.write("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nhello!".getBytes(StandardCharsets.UTF_8));
              }
              catch (Exception e) {
                // Suppress
              }
            }
          }
        }
    );

    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final StatusResponseHolder response = client
          .go(
              new Request(
                  HttpMethod.GET,
                  new URL(StringUtils.format("http://localhost:%d/", serverSocket.getLocalPort()))
              ),
              StatusResponseHandler.getInstance()
          ).get();

      Assert.assertEquals(200, response.getStatus().getCode());
      Assert.assertEquals("hello!", response.getContent());
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }

  @Test
  public void testCompressionCodecConfig() throws Exception
  {
    final ExecutorService exec = Executors.newSingleThreadExecutor();
    final ServerSocket serverSocket = new ServerSocket(0);
    final AtomicBoolean foundAcceptEncoding = new AtomicBoolean();
    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = serverSocket.accept();
                  BufferedReader in = new BufferedReader(
                      new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8)
                  );
                  OutputStream out = clientSocket.getOutputStream()
              ) {
                // Read headers
                String header;
                while (!(header = in.readLine()).equals("")) {
                  if ("Accept-Encoding: identity".equals(header)) {
                    foundAcceptEncoding.set(true);
                  }
                }
                out.write("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nhello!".getBytes(StandardCharsets.UTF_8));
              }
              catch (Exception e) {
                // Suppress
              }
            }
          }
        }
    );

    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder()
                                                      .withCompressionCodec(HttpClientConfig.CompressionCodec.IDENTITY)
                                                      .build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final StatusResponseHolder response = client
          .go(
              new Request(
                  HttpMethod.GET,
                  new URL(StringUtils.format("http://localhost:%d/", serverSocket.getLocalPort()))
              ),
              StatusResponseHandler.getInstance()
          ).get();

      Assert.assertEquals(200, response.getStatus().getCode());
      Assert.assertEquals("hello!", response.getContent());
      Assert.assertTrue(foundAcceptEncoding.get());
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }

  @Test
  public void testFriendlySelfSignedHttpsServer() throws Exception
  {
    final Lifecycle lifecycle = new Lifecycle();
    final String keyStorePath = getClass().getClassLoader().getResource("keystore.jks").getFile();
    Server server = new Server();

    HttpConfiguration https = new HttpConfiguration();
    https.addCustomizer(new SecureRequestCustomizer());

    SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
    sslContextFactory.setKeyStorePath(keyStorePath);
    sslContextFactory.setKeyStorePassword("abc123");
    sslContextFactory.setKeyManagerPassword("abc123");

    ServerConnector sslConnector = new ServerConnector(
        server,
        new SslConnectionFactory(sslContextFactory, "http/1.1"),
        new HttpConnectionFactory(https)
    );

    sslConnector.setPort(0);
    server.setConnectors(new Connector[]{sslConnector});
    server.start();

    try {
      final SSLContext mySsl = HttpClientInit.sslContextWithTrustedKeyStore(keyStorePath, "abc123");
      final HttpClientConfig trustingConfig = HttpClientConfig.builder().withSslContext(mySsl).build();
      final HttpClient trustingClient = HttpClientInit.createClient(trustingConfig, lifecycle);

      final HttpClientConfig skepticalConfig = HttpClientConfig.builder()
                                                               .withSslContext(SSLContext.getDefault())
                                                               .build();
      final HttpClient skepticalClient = HttpClientInit.createClient(skepticalConfig, lifecycle);

      // Correct name ("localhost")
      {
        final HttpResponseStatus status = trustingClient
            .go(
                new Request(
                    HttpMethod.GET,
                    new URL(StringUtils.format("https://localhost:%d/", sslConnector.getLocalPort()))
                ),
                StatusResponseHandler.getInstance()
            ).get().getStatus();
        Assert.assertEquals(404, status.getCode());
      }

      // Incorrect name ("127.0.0.1")
      {
        final ListenableFuture<StatusResponseHolder> response1 = trustingClient
            .go(
                new Request(
                    HttpMethod.GET,
                    new URL(StringUtils.format("https://127.0.0.1:%d/", sslConnector.getLocalPort()))
                ),
                StatusResponseHandler.getInstance()
            );

        Throwable ea = null;
        try {
          response1.get();
        }
        catch (ExecutionException e) {
          ea = e.getCause();
        }

        Assert.assertTrue("ChannelException thrown by 'get'", ea instanceof ChannelException);
        Assert.assertTrue("Expected error message", ea.getCause().getMessage().contains("Failed to handshake"));
      }

      {
        // Untrusting client
        final ListenableFuture<StatusResponseHolder> response2 = skepticalClient
            .go(
                new Request(
                    HttpMethod.GET,
                    new URL(StringUtils.format("https://localhost:%d/", sslConnector.getLocalPort()))
                ),
                StatusResponseHandler.getInstance()
            );

        Throwable eb = null;
        try {
          response2.get();
        }
        catch (ExecutionException e) {
          eb = e.getCause();
        }
        Assert.assertNotNull("ChannelException thrown by 'get'", eb);
        Assert.assertTrue(
            "Root cause is SSLHandshakeException",
            eb.getCause().getCause() instanceof SSLHandshakeException
        );
      }
    }
    finally {
      lifecycle.stop();
      server.stop();
    }
  }

  @Test
  @Ignore
  public void testHttpBin() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withSslContext(SSLContext.getDefault()).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);

      {
        final HttpResponseStatus status = client
            .go(
                new Request(HttpMethod.GET, new URL("https://httpbin.org/get")),
                StatusResponseHandler.getInstance()
            ).get().getStatus();

        Assert.assertEquals(200, status.getCode());
      }

      {
        final HttpResponseStatus status = client
            .go(
                new Request(HttpMethod.POST, new URL("https://httpbin.org/post"))
                    .setContent(new byte[]{'a', 'b', 'c', 1, 2, 3}),
                StatusResponseHandler.getInstance()
            ).get().getStatus();

        Assert.assertEquals(200, status.getCode());
      }
    }
    finally {
      lifecycle.stop();
    }
  }
}
