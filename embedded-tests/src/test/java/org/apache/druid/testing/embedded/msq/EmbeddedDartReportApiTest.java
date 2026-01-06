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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.guice.SleepModule;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.dart.controller.http.DartQueryInfo;
import org.apache.druid.msq.dart.controller.sql.DartSqlClients;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.sql.http.GetReportResponse;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

/**
 * Embedded test for the Dart report API at {@code /druid/v2/sql/queries/{id}/reports}.
 * Uses batch ingestion to avoid dependency on Kafka/Docker.
 */
public class EmbeddedDartReportApiTest extends EmbeddedClusterTestBase
{
  private static final int MAX_RETAINED_REPORT_COUNT = 10;

  private final EmbeddedBroker broker1 = new EmbeddedBroker();
  private final EmbeddedBroker broker2 = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  private EmbeddedMSQApis msqApis;
  private String ingestedDataSource;

  private void configureBroker(EmbeddedBroker broker, int port)
  {
    broker.addProperty("druid.msq.dart.controller.heapFraction", "0.5")
          .addProperty("druid.msq.dart.controller.maxRetainedReportCount", String.valueOf(MAX_RETAINED_REPORT_COUNT))
          .addProperty("druid.query.default.context.maxConcurrentStages", "1")
          .addProperty("druid.plaintextPort", String.valueOf(port));
  }

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");
    overlord.addProperty("druid.manager.segments.pollDuration", "PT0.1s");

    // Enable Dart with report retention on both brokers, with different ports
    configureBroker(broker1, 7082);
    configureBroker(broker2, 7083);

    historical.addProperty("druid.msq.dart.worker.heapFraction", "0.5")
              .addProperty("druid.msq.dart.worker.concurrentQueries", "1");

    indexer.setServerMemory(400_000_000)
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
           .addProperty("druid.processing.numThreads", "2")
           .addProperty("druid.worker.capacity", "4");

    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addCommonProperty("druid.msq.dart.enabled", "true")
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(broker1)
                               .addServer(broker2)
                               .addServer(indexer)
                               .addServer(historical)
                               .addExtension(SleepModule.class);
  }

  @BeforeAll
  protected void setupData()
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);

    // Ingest test data once, using batch ingestion.
    ingestedDataSource = EmbeddedClusterApis.createTestDatasourceName();
    final String taskId = IdUtils.getRandomId();
    final IndexTask task = MoreResources.Task.BASIC_INDEX.get().dataSource(ingestedDataSource).withId(taskId);
    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);

    // Wait for segments to be available on both brokers
    cluster.callApi().waitForAllSegmentsToBeAvailable(ingestedDataSource, coordinator, broker1);
    cluster.callApi().waitForAllSegmentsToBeAvailable(ingestedDataSource, coordinator, broker2);
  }

  @Test
  @Timeout(60)
  public void test_getQueryReport_forCompletedDartQuery()
  {
    final String sqlQueryId = UUID.randomUUID().toString();
    final String sql = StringUtils.format("SELECT COUNT(*) FROM \"%s\"", ingestedDataSource);

    // Run a Dart query with a specific SQL query ID
    final String result = cluster.callApi().onAnyBroker(
        b -> b.submitSqlQuery(
            new ClientSqlQuery(
                sql,
                "CSV",
                false,
                false,
                false,
                ImmutableMap.of(
                    QueryContexts.ENGINE, "msq-dart",
                    QueryContexts.CTX_SQL_QUERY_ID, sqlQueryId
                ),
                null
            )
        )
    ).trim();

    // Verify the query returned results (should be 10 rows based on CSV_10_DAYS data)
    Assertions.assertEquals("10", result);

    // Now fetch the report using the SQL query ID
    final GetReportResponse reportResponse = msqApis.getDartQueryReport(sqlQueryId, broker1);

    // Verify the report response
    Assertions.assertNotNull(reportResponse, "Report response should not be null");
    Assertions.assertNotNull(reportResponse.getQueryInfo(), "Query info should not be null");
    Assertions.assertNotNull(reportResponse.getReportMap(), "Report should not be null");

    // Verify the query info
    final DartQueryInfo queryInfo = (DartQueryInfo) reportResponse.getQueryInfo();
    Assertions.assertEquals(sql, queryInfo.getSql());
    Assertions.assertEquals(sqlQueryId, queryInfo.getSqlQueryId());
    Assertions.assertNotNull(queryInfo.getDartQueryId());

    // Verify the report is an MSQTaskReport
    Assertions.assertInstanceOf(TaskReport.ReportMap.class, reportResponse.getReportMap());
    Assertions.assertInstanceOf(MSQTaskReport.class, reportResponse.getReportMap().get(MSQTaskReport.REPORT_KEY));
  }

  @Test
  @Timeout(60)
  public void test_getQueryReport_notFound()
  {
    // Try to get a report for a non-existent query
    final GetReportResponse reportResponse = msqApis.getDartQueryReport("nonexistent-query-id", broker1);

    // Verify the response is null (not found)
    Assertions.assertNull(reportResponse, "Report response should be null for non-existent query");
  }

  @Test
  @Timeout(60)
  public void test_getQueryReport_fromBothBrokers()
  {
    final String sqlQueryId = UUID.randomUUID().toString();
    final String sql = StringUtils.format("SELECT COUNT(*) FROM \"%s\"", ingestedDataSource);

    // Run a Dart query on any broker
    final String result = cluster.callApi().onAnyBroker(
        b -> b.submitSqlQuery(
            new ClientSqlQuery(
                sql,
                "CSV",
                false,
                false,
                false,
                ImmutableMap.of(
                    QueryContexts.ENGINE, "msq-dart",
                    QueryContexts.CTX_SQL_QUERY_ID, sqlQueryId
                ),
                null
            )
        )
    ).trim();

    // Verify the query returned results
    Assertions.assertEquals("10", result);

    // Verify both brokers have discovered each other
    final var sqlClients1 = broker1.bindings().getInstance(DartSqlClients.class);
    final var sqlClients2 = broker2.bindings().getInstance(DartSqlClients.class);
    Assertions.assertEquals(1, sqlClients1.getAllClients().size(), "Broker1 should have 1 client (broker2)");
    Assertions.assertEquals(1, sqlClients2.getAllClients().size(), "Broker2 should have 1 client (broker1)");

    // Fetch the report from both brokers, to verify cross-broker lookup is working
    final GetReportResponse reportFromBroker1 = msqApis.getDartQueryReport(sqlQueryId, broker1);
    final GetReportResponse reportFromBroker2 = msqApis.getDartQueryReport(sqlQueryId, broker2);

    // Verify the report content
    for (GetReportResponse report : Arrays.asList(reportFromBroker1, reportFromBroker2)) {
      Assertions.assertNotNull(report);
      final DartQueryInfo queryInfo = (DartQueryInfo) report.getQueryInfo();
      Assertions.assertEquals(sqlQueryId, queryInfo.getSqlQueryId());
      Assertions.assertEquals(sql, queryInfo.getSql());
      Assertions.assertNotNull(queryInfo.getDartQueryId());
      Assertions.assertInstanceOf(TaskReport.ReportMap.class, report.getReportMap());
      Assertions.assertInstanceOf(MSQTaskReport.class, report.getReportMap().get(MSQTaskReport.REPORT_KEY));
    }
  }

  @Test
  @Timeout(60)
  public void test_getQueryReport_forRunningAndCanceledQuery()
  {
    final String sqlQueryId = UUID.randomUUID().toString();

    // Use SLEEP to make the query run for a while.
    // Need to use a non-constant expression to make this happen at runtime rather than planning time.
    final String sql =
        StringUtils.format("SELECT SLEEP(TIMESTAMP_TO_MILLIS(__time) * 0 + 60) FROM \"%s\"", ingestedDataSource);

    // Step 1: Issue the query asynchronously.
    final ListenableFuture<String> queryFuture =
        msqApis.submitDartSqlAsync(sql, Map.of(QueryContexts.CTX_SQL_QUERY_ID, sqlQueryId), broker1);

    // Step 2: Get the report.
    final GetReportResponse runningReport = waitForReport(sqlQueryId);

    Assertions.assertNotNull(runningReport, "Report should be available for running query");
    Assertions.assertNotNull(runningReport.getQueryInfo(), "Query info should not be null");
    Assertions.assertNotNull(runningReport.getReportMap(), "Report should not be null");

    // Verify the query info
    final DartQueryInfo runningQueryInfo = (DartQueryInfo) runningReport.getQueryInfo();
    Assertions.assertEquals(sql, runningQueryInfo.getSql());
    Assertions.assertEquals(sqlQueryId, runningQueryInfo.getSqlQueryId());

    // Verify the report is an MSQTaskReport with RUNNING status
    final MSQTaskReport runningMsqReport =
        (MSQTaskReport) runningReport.getReportMap().get(MSQTaskReport.REPORT_KEY);
    Assertions.assertNotNull(runningMsqReport, "MSQ report should not be null");

    final MSQTaskReportPayload runningPayload = runningMsqReport.getPayload();
    Assertions.assertNotNull(runningPayload, "Payload should not be null");
    Assertions.assertNotNull(runningPayload.getStatus(), "Status should not be null");
    Assertions.assertEquals(
        TaskState.RUNNING,
        runningPayload.getStatus().getStatus(),
        "Query should be in RUNNING state"
    );

    // Step 3: Cancel the query
    final boolean canceled = msqApis.cancelDartQuery(sqlQueryId, broker1);
    Assertions.assertTrue(canceled, "Query cancellation should be accepted");

    // Step 4: Wait for the sqlQuery/time metric to be emitted (signals query completion)
    final LatchableEmitter emitter = broker1.latchableEmitter();
    emitter.waitForEvent(
        event -> event.hasMetricName("sqlQuery/time")
                      .hasDimension("id", sqlQueryId)
    );

    // Step 5: Fetch the report again - should now be in FAILED state
    final GetReportResponse canceledReport = msqApis.getDartQueryReport(sqlQueryId, broker1);

    Assertions.assertNotNull(canceledReport, "Report should be available for canceled query");
    Assertions.assertNotNull(canceledReport.getReportMap(), "Report map should not be null");

    final MSQTaskReport canceledMsqReport =
        (MSQTaskReport) canceledReport.getReportMap().get(MSQTaskReport.REPORT_KEY);
    Assertions.assertNotNull(canceledMsqReport, "MSQ report should not be null for canceled query");

    final MSQTaskReportPayload canceledPayload = canceledMsqReport.getPayload();
    Assertions.assertNotNull(canceledPayload, "Payload should not be null for canceled query");
    Assertions.assertNotNull(canceledPayload.getStatus(), "Status should not be null for canceled query");
    Assertions.assertEquals(
        TaskState.FAILED,
        canceledPayload.getStatus().getStatus(),
        "canceled query should be in FAILED state"
    );

    // The query future should complete with an error due to cancellation
    try {
      queryFuture.get();
      Assertions.fail("Query should have failed due to cancellation");
    }
    catch (Exception ignored) {
      // Expected - query was canceled
    }
  }

  /**
   * Polls the report API until a report is available.
   */
  private GetReportResponse waitForReport(String sqlQueryId)
  {
    final long timeout = 30_000;
    final long deadline = System.currentTimeMillis() + timeout;
    while (System.currentTimeMillis() < deadline) {
      final GetReportResponse report = msqApis.getDartQueryReport(sqlQueryId, broker1);
      if (report != null) {
        return report;
      }
      try {
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    throw new ISE("Timed out after[%,d] ms waiting for query to be in RUNNING state", timeout);
  }
}
