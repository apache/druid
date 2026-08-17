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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.utils.SocketUtil;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DirectDruidClientAbortHttpTest
{
  @Test
  public void testEarlyStreamClose() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();

    final AtomicBoolean serverSawDisconnect = new AtomicBoolean(false);
    final CountDownLatch responseSent = new CountDownLatch(1);
    final CountDownLatch connectionTerminated = new CountDownLatch(1);
    final CountDownLatch terminationDetected = new CountDownLatch(1);

    final int port = SocketUtil.findOpenPort(0);
    final Server server = new Server(port);
    final ServletContextHandler handler = new ServletContextHandler();
    handler.addServlet(
        new ServletHolder(new HttpServlet()
        {
          @Override
          protected void doPost(HttpServletRequest req, HttpServletResponse resp)
          {
            final ServletOutputStream out;
            try {
              out = resp.getOutputStream();
              resp.setStatus(HttpServletResponse.SC_OK);
              out.write(StringUtils.toUtf8("[{\"timestamp\":\"2014-01-01T01:02:03Z\", \"result\": 42.0}]"));
              out.flush();
            }
            catch (Exception e) {
              throw DruidException.defensive(e, "Encountered some issue while sending the real bytes");
            }

            responseSent.countDown();

            try {
              connectionTerminated.await();
              final long endTime = System.currentTimeMillis() + 100;
              while (System.currentTimeMillis() < endTime) {
                out.write(0);
                out.flush();
              }
            }
            catch (IOException e) {
              serverSawDisconnect.set(true);
              terminationDetected.countDown();
            }
            catch (Exception e) {
              throw DruidException.defensive(
                  e,
                  "Encountered some issue while awaiting for the connection to be terminated"
              );
            }
          }
        }),
        "/*"
    );
    server.setHandler(handler);

    final Lifecycle lifecycle = new Lifecycle();
    final ScheduledExecutorService queryCancellationExecutor =
        Execs.scheduledSingleThreaded("DirectDruidClientAbortHttpTest-cancel-%d");
    final Closer closer = Closer.create();
    try {
      server.start();
      lifecycle.start();

      final QueryRunnerFactoryConglomerate conglomerate =
          QueryStackTests.createQueryRunnerFactoryConglomerate(closer);

      final DirectDruidClient directDruidClient = new DirectDruidClient(
          conglomerate,
          QueryRunnerTestHelper.NOOP_QUERYWATCHER,
          mapper,
          HttpClientInit.createClient(
              HttpClientConfig.builder().withNumConnections(1).build(),
              lifecycle
          ),
          "http",
          "localhost:" + port,
          new NoopServiceEmitter(),
          queryCancellationExecutor
      );

      final Map<String, Object> queryContext = ImmutableMap.of(
          DirectDruidClient.QUERY_FAIL_TIME, System.currentTimeMillis() + 60_000L,
          BaseQuery.QUERY_ID, "abort-test"
      );

      final Sequence results = directDruidClient.run(
          QueryPlus.wrap(Druids.newTimeBoundaryQueryBuilder().dataSource("test").context(queryContext).build()),
          DirectDruidClient.makeResponseContextForQuery()
      );

      responseSent.await();

      final AtomicInteger resultCount = new AtomicInteger(0);
      final Yielder yielder = Yielders.each(results);
      try {
        Assertions.assertFalse(yielder.isDone(), "expected at least one result before stopping");
        final Result result = (Result) yielder.get();
        Assertions.assertEquals(DateTimes.of("2014-01-01T01:02:03Z"), result.getTimestamp());
        resultCount.incrementAndGet();
      }
      finally {
        connectionTerminated.countDown();
        yielder.close();
      }

      Assertions.assertEquals(1, resultCount.get(), "expected exactly one result before stopping");
      if (!terminationDetected.await(5, TimeUnit.SECONDS)) {
        Assertions.fail("Test did not complete in 5 seconds!?");
      }
      Assertions.assertTrue(serverSawDisconnect.get(), "server should have marked the connection as disconnected");
    }
    finally {
      queryCancellationExecutor.shutdownNow();
      closer.close();
      lifecycle.stop();
      server.stop();
    }
  }
}
