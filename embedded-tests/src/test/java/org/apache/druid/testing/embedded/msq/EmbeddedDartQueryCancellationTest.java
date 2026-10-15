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

package org.apache.druid.testing.embedded.msq;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.QueryContexts;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Embedded tests for Dart query cancellation.
 */
public class EmbeddedDartQueryCancellationTest extends BaseDartQueryTest
{
  @Test
  @Timeout(value = 180, unit = TimeUnit.SECONDS)
  public void test_cancelDartQuery_cancelsWorkOrderRetries() throws Exception
  {
    final String sqlQueryId = UUID.randomUUID().toString();
    final String sql = StringUtils.format(
        "SELECT SLEEP(TIMESTAMP_TO_MILLIS(__time) * 0 + 60) FROM \"%s\"",
        ingestedDataSource
    );
    final CountDownLatch workOrderAttempts = new CountDownLatch(2);
    final CountDownLatch workOrderAfterCancellation = new CountDownLatch(1);
    final AtomicBoolean cancellationCompleted = new AtomicBoolean();

    // Drop worker responses so the controller keeps retrying /workOrder.
    try (HistoricalBlackhole blackhole = new HistoricalBlackhole(
        false,
        true,
        workOrderAttempts,
        workOrderAfterCancellation,
        cancellationCompleted
    )) {
      final ListenableFuture<String> queryFuture = msqApis.submitDartSqlAsync(
          sql,
          Map.of(QueryContexts.CTX_SQL_QUERY_ID, sqlQueryId),
          broker1
      );
      try {
        waitForReport(sqlQueryId);

        Assertions.assertTrue(
            workOrderAttempts.await(15, TimeUnit.SECONDS),
            "Dart controller did not retry /workOrder"
        );

        // Cancel while the /workOrder request is in flight.
        final ListenableFuture<Boolean> cancellation = cluster.callApi().onTargetBrokerAsync(
            broker1,
            broker -> broker.cancelSqlQuery(sqlQueryId)
        );

        Assertions.assertTrue(cancellation.get(30, TimeUnit.SECONDS));
        cancellationCompleted.set(true);

        Assertions.assertFalse(
            workOrderAfterCancellation.await(5, TimeUnit.SECONDS),
            "Dart controller sent /workOrder after cancellation"
        );
      }
      finally {
        queryFuture.cancel(true);
      }
    }
  }

  /**
   * Replaces the historical HTTP connector with a socket that selectively acknowledges worker requests.
   */
  private class HistoricalBlackhole implements AutoCloseable
  {
    private final boolean acknowledgeStop;
    private final boolean acknowledgeWorkOrder;
    private final CountDownLatch workOrderAttempts;
    private final CountDownLatch workOrderAfterCancellation;
    private final AtomicBoolean cancellationCompleted;
    private final Connector connector;
    private final ServerSocket socket;
    private final ExecutorService executor;
    private boolean restored;

    private HistoricalBlackhole(
        final boolean acknowledgeWorkOrder,
        final boolean acknowledgeStop,
        final CountDownLatch workOrderAttempts,
        final CountDownLatch workOrderAfterCancellation,
        final AtomicBoolean cancellationCompleted
    ) throws Exception
    {
      this.acknowledgeWorkOrder = acknowledgeWorkOrder;
      this.acknowledgeStop = acknowledgeStop;
      this.workOrderAttempts = workOrderAttempts;
      this.workOrderAfterCancellation = workOrderAfterCancellation;
      this.cancellationCompleted = cancellationCompleted;
      this.connector = historical.bindings().getInstance(Server.class).getConnectors()[0];

      final int port = ((NetworkConnector) connector).getLocalPort();
      // Keep the Historical announced and replace only its HTTP endpoint.
      connector.stop();

      this.socket = new ServerSocket(port);
      this.executor = Execs.singleThreaded("EmbeddedDartQueryCancellationTest-blackhole-%s");
      this.executor.submit(this::acceptRequests);
    }

    private void acceptRequests()
    {
      while (!socket.isClosed()) {
        try (Socket request = socket.accept()) {
          request.setSoTimeout(1_000);
          final String requestLine = new BufferedReader(
              new InputStreamReader(request.getInputStream(), StandardCharsets.UTF_8)
          ).readLine();

          final boolean workOrder = requestLine != null && requestLine.contains("/workOrder");
          final boolean stop = requestLine != null && requestLine.contains("/stop");

          if (workOrder) {
            if (cancellationCompleted.get()) {
              workOrderAfterCancellation.countDown();
            } else {
              workOrderAttempts.countDown();
            }
          }

          if ((workOrder && acknowledgeWorkOrder) || (stop && acknowledgeStop)) {
            final OutputStream output = request.getOutputStream();
            output.write(
                "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .getBytes(StandardCharsets.UTF_8)
            );
            output.flush();
          }
        }
        catch (final IOException e) {
          if (!socket.isClosed()) {
            // The client can close a request while cancellation is racing with a retry.
          }
        }
      }
    }

    @Override
    public void close() throws Exception
    {
      if (!restored) {
        restored = true;
        socket.close();
        executor.shutdownNow();
        if (!connector.isRunning()) {
          connector.start();
        }
      }
    }
  }
}
