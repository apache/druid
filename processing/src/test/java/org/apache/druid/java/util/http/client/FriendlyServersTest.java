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
import io.netty.channel.ChannelException;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
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
import org.eclipse.jetty.util.ssl.KeyStoreScanner;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

      Assert.assertEquals(200, response.getStatus().code());
      Assert.assertEquals("hello!", response.getContent());
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }

  @Test
  public void testFriendlyProxyHttpServer() throws Exception
  {
    final AtomicReference<String> requestContent = new AtomicReference<>();

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
                StringBuilder request = new StringBuilder();
                String line;
                while (!"".equals((line = in.readLine()))) {
                  request.append(line).append("\r\n");
                }
                requestContent.set(request.toString());
                out.write("HTTP/1.1 200 OK\r\n\r\n".getBytes(StandardCharsets.UTF_8));

                while (!in.readLine().equals("")) {
                  // skip lines
                }
                out.write("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nhello!".getBytes(StandardCharsets.UTF_8));
              }
              catch (Exception e) {
                Assert.fail(e.toString());
              }
            }
          }
        }
    );

    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig
          .builder()
          .withHttpProxyConfig(
              new HttpClientProxyConfig("localhost", serverSocket.getLocalPort(), "bob", "sally")
          )
          .build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final StatusResponseHolder response = client
          .go(
              new Request(
                  HttpMethod.GET,
                  new URL("http://anotherHost:8080/")
              ),
              StatusResponseHandler.getInstance()
          ).get();

      Assert.assertEquals(200, response.getStatus().code());
      Assert.assertEquals("hello!", response.getContent());

      Assert.assertEquals(
          "CONNECT anotherHost:8080 HTTP/1.1\r\nProxy-Authorization: Basic Ym9iOnNhbGx5\r\n",
          requestContent.get()
      );
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
                // Read headers (HTTP header names are case-insensitive; Netty 4 emits lowercase.)
                String header;
                while (!(header = in.readLine()).equals("")) {
                  if ("accept-encoding: identity".equals(header.toLowerCase(Locale.ROOT))) {
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

      Assert.assertEquals(200, response.getStatus().code());
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
    KeyStoreScanner keyStoreScanner = new KeyStoreScanner(sslContextFactory);
    server.addBean(keyStoreScanner);
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
        Assert.assertEquals(404, status.code());
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

        Assert.assertEquals(200, status.code());
      }

      {
        final HttpResponseStatus status = client
            .go(
                new Request(HttpMethod.POST, new URL("https://httpbin.org/post"))
                    .setContent(new byte[]{'a', 'b', 'c', 1, 2, 3}),
                StatusResponseHandler.getInstance()
            ).get().getStatus();

        Assert.assertEquals(200, status.code());
      }
    }
    finally {
      lifecycle.stop();
    }
  }

  /**
   * Regression test for the case where the same {@link Request} (or a copy of it) is sent more
   * than once. Netty 4's HttpObjectEncoder advances the content's reader index and releases
   * {@link io.netty.handler.codec.http.FullHttpRequest} after encoding; if {@link NettyHttpClient}
   * handed the Request's stored {@link io.netty.buffer.ByteBuf} directly to the encoder, the
   * second send would either drain to zero bytes or hit an {@link io.netty.util.IllegalReferenceCountException}.
   *
   * KerberosHttpClient hits this path: on a 401 it calls {@code Request.copy()} (which deep-copies
   * the content) and re-sends. This test exercises the same shape.
   */
  @Test
  public void testRequestCanBeSentMultipleTimes() throws Exception
  {
    final ExecutorService exec = Executors.newCachedThreadPool();
    final ServerSocket serverSocket = new ServerSocket(0);
    final BlockingQueue<String> receivedBodies = new LinkedBlockingQueue<>();
    // Keep-alive HTTP/1.1 loop: each accepted connection serves consecutive requests until the
    // peer disconnects. Needed because the client's channel pool reuses connections, and a
    // single-shot server would cause a write-to-closed-channel race on the second send.
    exec.submit(
        () -> {
          while (!Thread.currentThread().isInterrupted()) {
            final Socket clientSocket;
            try {
              clientSocket = serverSocket.accept();
            }
            catch (IOException e) {
              return;
            }
            exec.submit(() -> {
              try (
                  Socket s = clientSocket;
                  BufferedReader in = new BufferedReader(
                      new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8)
                  );
                  OutputStream out = s.getOutputStream()
              ) {
                while (true) {
                  String requestLine = in.readLine();
                  if (requestLine == null) {
                    return;
                  }
                  int contentLength = 0;
                  String header;
                  while (!(header = in.readLine()).equals("")) {
                    if (header.toLowerCase(Locale.ROOT).startsWith("content-length:")) {
                      contentLength = Integer.parseInt(header.substring("content-length:".length()).trim());
                    }
                  }
                  char[] body = new char[contentLength];
                  int read = 0;
                  while (read < contentLength) {
                    int n = in.read(body, read, contentLength - read);
                    if (n < 0) {
                      return;
                    }
                    read += n;
                  }
                  receivedBodies.put(new String(body, 0, read));
                  out.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".getBytes(StandardCharsets.UTF_8));
                  out.flush();
                }
              }
              catch (Exception ignored) {
                // suppress
              }
            });
          }
        }
    );

    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClient client = HttpClientInit.createClient(HttpClientConfig.builder().build(), lifecycle);
      final URL url = new URL(StringUtils.format("http://localhost:%d/", serverSocket.getLocalPort()));
      final byte[] payload = "hello-body".getBytes(StandardCharsets.UTF_8);

      final Request request = new Request(HttpMethod.POST, url).setContent(payload);

      // First send.
      Assert.assertEquals(
          200,
          client.go(request, StatusResponseHandler.getInstance()).get().getStatus().code()
      );

      // KerberosHttpClient's retry pattern: copy the request and send the copy. This must not
      // observe a released ByteBuf nor an already-drained reader index on the original.
      Assert.assertEquals(
          200,
          client.go(request.copy(), StatusResponseHandler.getInstance()).get().getStatus().code()
      );

      // And the original Request must still be sendable a third time on its own.
      Assert.assertEquals(
          200,
          client.go(request, StatusResponseHandler.getInstance()).get().getStatus().code()
      );

      Assert.assertEquals("hello-body", receivedBodies.poll(10, TimeUnit.SECONDS));
      Assert.assertEquals("hello-body", receivedBodies.poll(10, TimeUnit.SECONDS));
      Assert.assertEquals("hello-body", receivedBodies.poll(10, TimeUnit.SECONDS));
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }

  /**
   * Regression test for the case where a caller sets HTTP header names using their conventional
   * casing (e.g. {@code Host}, {@code Accept-Encoding}) rather than the lowercase form Netty 4's
   * {@link HttpHeaderNames} constants use. NettyHttpClient must not emit duplicate headers when
   * these overlap with defaults it would otherwise apply.
   */
  @Test
  public void testCallerHostHeaderDoesNotDuplicate() throws Exception
  {
    final ExecutorService exec = Executors.newSingleThreadExecutor();
    final ServerSocket serverSocket = new ServerSocket(0);
    final BlockingQueue<java.util.List<String>> receivedHeaders = new LinkedBlockingQueue<>();
    exec.submit(
        () -> {
          while (!Thread.currentThread().isInterrupted()) {
            try (
                Socket clientSocket = serverSocket.accept();
                BufferedReader in = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8)
                );
                OutputStream out = clientSocket.getOutputStream()
            ) {
              in.readLine(); // request line
              java.util.List<String> hdrs = new java.util.ArrayList<>();
              String h;
              while (!(h = in.readLine()).isEmpty()) {
                hdrs.add(h);
              }
              receivedHeaders.put(hdrs);
              out.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".getBytes(StandardCharsets.UTF_8));
            }
            catch (Exception ignored) {
              // suppress
            }
          }
        }
    );

    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClient client = HttpClientInit.createClient(HttpClientConfig.builder().build(), lifecycle);
      final URL url = new URL(StringUtils.format("http://localhost:%d/", serverSocket.getLocalPort()));

      // Caller sets Host and Accept-Encoding using conventional (title-case) HTTP header names.
      // Netty 4's HttpHeaderNames constants are lowercase; a naive case-sensitive presence check
      // against the caller's Multimap would miss these and cause NettyHttpClient to add duplicates.
      final Request request = new Request(HttpMethod.GET, url)
          .setHeader("Host", "override.example.com")
          .setHeader("Accept-Encoding", "identity");

      Assert.assertEquals(
          200,
          client.go(request, StatusResponseHandler.getInstance()).get().getStatus().code()
      );

      final java.util.List<String> hdrs = receivedHeaders.poll(10, TimeUnit.SECONDS);
      Assert.assertNotNull("no headers received", hdrs);

      int hostCount = 0;
      int acceptEncodingCount = 0;
      for (String hdr : hdrs) {
        final String name = hdr.substring(0, hdr.indexOf(':')).toLowerCase(Locale.ROOT);
        if ("host".equals(name)) {
          hostCount++;
          Assert.assertTrue(
              "Host header should reflect caller value, was: " + hdr,
              hdr.toLowerCase(Locale.ROOT).contains("override.example.com")
          );
        } else if ("accept-encoding".equals(name)) {
          acceptEncodingCount++;
          Assert.assertTrue(
              "Accept-Encoding should be caller-supplied identity, was: " + hdr,
              hdr.toLowerCase(Locale.ROOT).contains("identity")
          );
        }
      }
      Assert.assertEquals("exactly one Host header on the wire", 1, hostCount);
      Assert.assertEquals("exactly one Accept-Encoding header on the wire", 1, acceptEncodingCount);
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }
}
