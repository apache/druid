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
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

/**
 * Tests for {@link NettyHttpClient} exercising real socket I/O.
 */
public class NettyHttpClientTest
{
  /**
   * A response handler whose {@link #handleResponse} throws, simulating a handler that rejects the response (for
   * example because of an unexpected status code or content type) before any content has been processed.
   */
  private static class ThrowingResponseHandler implements HttpResponseHandler<Object, Object>
  {
    private final RuntimeException toThrow;

    ThrowingResponseHandler(RuntimeException toThrow)
    {
      this.toThrow = toThrow;
    }

    @Override
    public ClientResponse<Object> handleResponse(HttpResponse response, TrafficCop trafficCop)
    {
      throw toThrow;
    }

    @Override
    public ClientResponse<Object> handleChunk(ClientResponse<Object> clientResponse, HttpChunk chunk, long chunkNum)
    {
      return clientResponse;
    }

    @Override
    public ClientResponse<Object> done(ClientResponse<Object> clientResponse)
    {
      return ClientResponse.finished(clientResponse.getObj());
    }

    @Override
    public void exceptionCaught(ClientResponse<Object> clientResponse, Throwable e)
    {
      // Nothing to do.
    }
  }

  /**
   * Regression test: an exception thrown from {@link HttpResponseHandler#handleResponse} must fail the future
   * returned by {@link HttpClient#go}, not resolve it to {@code null}. Previously, {@link NettyHttpClient} would
   * call {@code retVal.set(null)} in its catch block before rethrowing, which meant the thrown exception was
   * discarded and callers observed a successful null result instead of a failure.
   */
  @Test
  public void testHandleResponseExceptionFailsFuture() throws Exception
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
                out.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}".getBytes(StandardCharsets.UTF_8));
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

      final RuntimeException expected = new RuntimeException("boom from handleResponse");
      final ListenableFuture<Object> future = client.go(
          new Request(
              HttpMethod.GET,
              new URL(StringUtils.format("http://localhost:%d/", serverSocket.getLocalPort()))
          ),
          new ThrowingResponseHandler(expected)
      );

      final ExecutionException e = Assertions.assertThrows(ExecutionException.class, future::get);
      Assertions.assertSame(expected, e.getCause(), "the exception thrown by handleResponse must not be lost");
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }
}
