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
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.msq.dart.worker.DartWorkerRunner;
import org.apache.druid.msq.dart.worker.http.DartWorkerInfo;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Embedded tests for how Historicals decide whether the Broker running a Dart query is still alive.
 */
public class EmbeddedDartLivenessTest extends BaseDartQueryTest
{
  private static final long SYNC_PERIOD_MILLIS = 100;
  private static final long SYNC_TIMEOUT_MILLIS = 1_000;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    // Must be positive for in-flight queries to finish after shutdown starts, since zero disables graceful stop.
    broker2.addProperty("druid.server.http.gracefulShutdownTimeout", "PT30S");

    // Shorten syncs and the sync timeout, so a sync that wrongly leaves a query out cancels it quickly.
    broker1.addProperty("druid.msq.dart.controller.syncPeriod", new Period(SYNC_PERIOD_MILLIS).toString());
    broker2.addProperty("druid.msq.dart.controller.syncPeriod", new Period(SYNC_PERIOD_MILLIS).toString());
    historical.addProperty("druid.msq.dart.worker.syncTimeout", new Period(SYNC_TIMEOUT_MILLIS).toString());

    return super.createCluster();
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void test_longQuery_notCanceledBySyncs() throws Exception
  {
    // Sleeps 0.3 seconds per row. Embedded Historicals have one processing thread, so the 10 rows run one at a time.
    final String sql = StringUtils.format(
        "SELECT COUNT(*) FROM \"%s\" WHERE SLEEP(TIMESTAMP_TO_MILLIS(__time) * 0 + 0.3) IS NULL",
        ingestedDataSource
    );

    final Stopwatch stopwatch = Stopwatch.createStarted();
    Assertions.assertEquals(
        "10",
        msqApis.submitDartSqlAsync(sql, Map.of(), broker1).get(30, TimeUnit.SECONDS).trim()
    );

    // The query must outlast a sync period plus the sync timeout, with margin, for a sync that wrongly left it out
    // to have canceled it.
    final long minDurationMillis = 2 * (SYNC_PERIOD_MILLIS + SYNC_TIMEOUT_MILLIS);
    Assertions.assertTrue(
        stopwatch.millisElapsed() >= minDurationMillis,
        StringUtils.format("Query took[%,d] ms, less than[%,d] ms", stopwatch.millisElapsed(), minDurationMillis)
    );
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void test_stopBroker_runningQueryFinishes() throws Exception
  {
    final String sqlQueryId = UUID.randomUUID().toString();

    // Use a non-constant SLEEP argument so it is evaluated at runtime rather than at planning time.
    final String sql = StringUtils.format(
        "SELECT COUNT(*) FROM \"%s\" WHERE SLEEP(TIMESTAMP_TO_MILLIS(__time) * 0 + 0.3) IS NULL",
        ingestedDataSource
    );

    final ListenableFuture<String> queryFuture =
        msqApis.submitDartSqlAsync(sql, QueryContext.ofMap(QueryContextParameters.SQL_QUERY_ID, sqlQueryId), broker2);

    final ExecutorService stopExec = Execs.singleThreaded("EmbeddedDartLivenessTest-stop-%s");
    Future<?> stopFuture = null;
    try {
      waitForReport(sqlQueryId, broker2);

      // Stop the Broker while the query is running. Historicals see it leave discovery right away, but the query
      // should still finish, since the Broker keeps serving it for up to druid.server.http.gracefulShutdownTimeout.
      stopFuture = stopExec.submit(() -> {
        broker2.stop();
        return null;
      });

      Assertions.assertEquals("10", queryFuture.get(30, TimeUnit.SECONDS).trim());
      stopFuture.get(30, TimeUnit.SECONDS);
    }
    finally {
      queryFuture.cancel(true);
      stopExec.shutdownNow();

      // Restart only if the stop finished; otherwise starting would race with it and hide the original failure.
      if (stopFuture != null && stopFuture.isDone()) {
        broker2.start();
      }
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void test_leftoverWorker_canceledBySyncs() throws Exception
  {
    // Start a worker on the Historical as if broker2 had run a query there, then lost track of it, for example
    // because its stop command was lost.
    final String broker2Host = broker2.bindings().selfNode().getHostAndPortToUse();
    final String leftoverQueryId = UUID.randomUUID().toString();
    getWorkerRunner().startWorker(leftoverQueryId, broker2Host, QueryContext.empty());
    Assertions.assertTrue(getWorkerQueryIds(broker2Host).contains(leftoverQueryId));

    // broker2 syncs the Historical even though it isn't running queries there, which cancels the worker. This must be
    // faster than druid.msq.dart.worker.controllerTimeout, which would cancel it anyway.
    awaitWorkerQueryIds(broker2Host, Set::isEmpty, 10_000);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void test_stopBroker_withoutGracefulShutdown_stopsWorkers() throws Exception
  {
    // Sleeps 3 seconds per row, so the query would take 30 seconds if allowed to finish.
    final String sql = StringUtils.format(
        "SELECT COUNT(*) FROM \"%s\" WHERE SLEEP(TIMESTAMP_TO_MILLIS(__time) * 0 + 3) IS NULL",
        ingestedDataSource
    );

    final String broker1Host = broker1.bindings().selfNode().getHostAndPortToUse();
    final ListenableFuture<String> queryFuture = msqApis.submitDartSqlAsync(sql, Map.of(), broker1);
    boolean stopped = false;

    try {
      awaitWorkerQueryIds(broker1Host, queryIds -> !queryIds.isEmpty(), 30_000);

      // broker1 has no graceful shutdown timeout, so stopping it cancels the query. The worker must exit right after,
      // since nothing else would cancel it before druid.msq.dart.worker.controllerTimeout.
      broker1.stop();
      stopped = true;
      awaitWorkerQueryIds(broker1Host, Set::isEmpty, 10_000);
    }
    finally {
      queryFuture.cancel(true);

      if (stopped) {
        broker1.start();
        cluster.callApi().waitForAllSegmentsToBeAvailable(ingestedDataSource, coordinator, broker1);
      }
    }
  }

  private DartWorkerRunner getWorkerRunner()
  {
    return historical.bindings().getInstance(DartWorkerRunner.class);
  }

  /**
   * Returns the query IDs of workers on the Historical that belong to the given controller.
   */
  private Set<String> getWorkerQueryIds(final String controllerHost)
  {
    return getWorkerRunner().getWorkersResponse()
                            .getWorkers()
                            .stream()
                            .filter(worker -> controllerHost.equals(worker.getControllerHost()))
                            .map(DartWorkerInfo::getDartQueryId)
                            .collect(Collectors.toSet());
  }

  /**
   * Polls {@link #getWorkerQueryIds} until it matches the given predicate, failing after the given timeout.
   */
  private void awaitWorkerQueryIds(
      final String controllerHost,
      final Predicate<Set<String>> predicate,
      final long timeoutMillis
  ) throws InterruptedException
  {
    final long deadline = System.currentTimeMillis() + timeoutMillis;
    Set<String> queryIds = getWorkerQueryIds(controllerHost);
    while (!predicate.test(queryIds)) {
      if (System.currentTimeMillis() > deadline) {
        Assertions.fail(
            StringUtils.format(
                "Workers for controller[%s] were %s after [%,d] ms",
                controllerHost,
                queryIds,
                timeoutMillis
            )
        );
      }

      Thread.sleep(100);
      queryIds = getWorkerQueryIds(controllerHost);
    }
  }
}
