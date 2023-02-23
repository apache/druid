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

import org.apache.druid.java.util.common.StringUtils;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AsyncHttpClientTest
{

  @Test
  public void testRequestTimeout() throws Exception
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
                Thread.sleep(5000); // times out
                out.write("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nhello!".getBytes(StandardCharsets.UTF_8));
              }
              catch (Exception e) {
                // Suppress
              }
            }
          }
        }
    );

    long requestStart = 0;
    DefaultAsyncHttpClient client = new DefaultAsyncHttpClient();
    // Creation of connection for the first time takes long time, probably because of DNS cache or something like that
    warmUp(serverSocket, client);
    try {
      requestStart = System.currentTimeMillis();
      Future<?> future = client
          .prepareGet(StringUtils.format("http://localhost:%d/", serverSocket.getLocalPort()))
          .setRequestTimeout(2000)
          .execute();
      System.out.println("created future in: " + (System.currentTimeMillis() - requestStart));
      future.get(3000, TimeUnit.MILLISECONDS);
      Assert.fail("Expected timeout");
    }
    catch (ExecutionException | TimeoutException e) {
      long elapsed = System.currentTimeMillis() - requestStart;
      // Within 10% of timeout
      Assert.assertTrue("elapsed: " + elapsed, elapsed < 2200);
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
    }
  }

  private void warmUp(ServerSocket serverSocket, DefaultAsyncHttpClient client)
  {
    try {
      Future<?> future = client
          .prepareGet(StringUtils.format("http://localhost:%d/", serverSocket.getLocalPort()))
          .setRequestTimeout(100)
          .execute();
      future.get();
    }
    catch (Exception e) {
      // ignore
    }
  }
}
