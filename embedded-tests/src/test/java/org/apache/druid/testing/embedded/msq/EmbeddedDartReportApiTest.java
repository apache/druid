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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.SleepModule;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.msq.dart.controller.http.DartQueryInfo;
import org.apache.druid.msq.dart.controller.sql.DartSqlClients;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.http.GetQueriesResponse;
import org.apache.druid.sql.http.GetQueryReportResponse;
import org.apache.druid.sql.http.QueryInfo;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.auth.EmbeddedBasicAuthResource;
import org.apache.druid.testing.embedded.auth.HttpUtil;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Embedded test for the Dart report API at {@code /druid/v2/sql/queries/{id}/reports}.
 * Uses batch ingestion to avoid dependency on Kafka/Docker.
 */
public class EmbeddedDartReportApiTest extends EmbeddedClusterTestBase
{
  private static final int MAX_RETAINED_REPORT_COUNT = 10;

  // Authentication constants - use shared constants from EmbeddedBasicAuthResource where available
  private static final String REGULAR_USER = "regularUser";
  private static final String REGULAR_PASSWORD = "helloworld";

  private final EmbeddedBroker broker1 = new EmbeddedBroker();
  private final EmbeddedBroker broker2 = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  private EmbeddedMSQApis msqApis;
  private String ingestedDataSource;
  private HttpClient adminClient;
  private HttpClient regularUserClient;

  private void configureBroker(EmbeddedBroker broker, int port)
  {
    broker.addProperty("druid.msq.dart.controller.heapFraction", "0.5")
          .addProperty("druid.msq.dart.controller.maxRetainedReportCount", String.valueOf(MAX_RETAINED_REPORT_COUNT))
          .addProperty("druid.query.default.context.maxConcurrentStages", "1")
          .addProperty("druid.sql.planner.enableSysQueriesTable", "true")
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
                               .addResource(new EmbeddedBasicAuthResource())
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

    // Set up HTTP clients for admin and regular user
    setupAdminClient();
    setupRegularUserAndClient();

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

  private void setupAdminClient()
  {
    adminClient = new CredentialedHttpClient(
        new BasicCredentials(EmbeddedBasicAuthResource.ADMIN_USER, EmbeddedBasicAuthResource.ADMIN_PASSWORD),
        broker1.bindings().globalHttpClient()
    );
  }

  /**
   * Creates a regular user with only datasource read permission (no STATE READ).
   * Username is {@link #REGULAR_USER}, password is {@link #REGULAR_PASSWORD}.
   */
  private void setupRegularUserAndClient()
  {
    // Grant permissions: datasource read access to all datasources, sys.queries table access, but no STATE READ
    final List<ResourceAction> permissions = ImmutableList.of(
        new ResourceAction(new Resource(".*", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("queries", ResourceType.SYSTEM_TABLE), Action.READ)
    );

    EmbeddedBasicAuthResource.createUserWithPermissions(
        adminClient,
        coordinator,
        REGULAR_USER,
        REGULAR_PASSWORD,
        "regularRole",
        permissions
    );

    regularUserClient = new CredentialedHttpClient(
        new BasicCredentials(REGULAR_USER, REGULAR_PASSWORD),
        broker1.bindings().globalHttpClient()
    );
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
    final GetQueryReportResponse reportResponse = msqApis.getDartQueryReport(sqlQueryId, broker1);

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
  public void test_sysQueries_returnsRecentlyFinishedQuery() throws IOException
  {
    final String sqlQueryId = UUID.randomUUID().toString();

    // Run a Dart query with a specific SQL query ID
    final String result = cluster.callApi().runSql(
        "SET engine = 'msq-dart';\n"
        + "SET sqlQueryId = '%s';\n"
        + "SELECT COUNT(*) FROM \"%s\"",
        sqlQueryId,
        ingestedDataSource
    );

    // Verify the query returned results.
    Assertions.assertEquals("10", result);

    // Query sys.queries to find the recently-finished query.
    final String sysQueriesText = cluster.callApi().runSql(
        "SELECT engine, state, info FROM sys.queries\n"
        + "WHERE engine = 'msq-dart'\n"
        + "AND info LIKE '%%%s%%'",
        sqlQueryId
    ).trim();

    // Verify the query appears in sys.queries with SUCCESS state.
    final String[] sysQueriesResult = CsvInputFormat.createOpenCsvParser().parseLine(sysQueriesText);

    Assertions.assertEquals("msq-dart", sysQueriesResult[0]);
    Assertions.assertEquals("SUCCESS", sysQueriesResult[1]);

    final DartQueryInfo sysQueriesQueryInfo = (DartQueryInfo) broker1.bindings().jsonMapper().readValue(
        sysQueriesResult[2],
        QueryInfo.class
    );

    Assertions.assertEquals(sqlQueryId, sysQueriesQueryInfo.getSqlQueryId());
  }

  @Test
  @Timeout(60)
  public void test_getQueryReport_notFound()
  {
    // Try to get a report for a non-existent query
    final GetQueryReportResponse reportResponse = msqApis.getDartQueryReport("nonexistent-query-id", broker1);

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
    final GetQueryReportResponse reportFromBroker1 = msqApis.getDartQueryReport(sqlQueryId, broker1);
    final GetQueryReportResponse reportFromBroker2 = msqApis.getDartQueryReport(sqlQueryId, broker2);

    // Verify the report content
    for (GetQueryReportResponse report : Arrays.asList(reportFromBroker1, reportFromBroker2)) {
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
    final GetQueryReportResponse runningReport = waitForReport(sqlQueryId);

    Assertions.assertNotNull(runningReport, "Report should be available for running query");
    Assertions.assertNotNull(runningReport.getQueryInfo(), "Query info should not be null");
    Assertions.assertNotNull(runningReport.getReportMap(), "Report should not be null");

    // Verify the query info
    final DartQueryInfo runningQueryInfo = (DartQueryInfo) runningReport.getQueryInfo();
    Assertions.assertEquals(sql, runningQueryInfo.getSql());
    Assertions.assertEquals(sqlQueryId, runningQueryInfo.getSqlQueryId());

    // Verify the report is an MSQTaskReport with RUNNING state
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
    final GetQueryReportResponse canceledReport = msqApis.getDartQueryReport(sqlQueryId, broker1);

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
   * Test that admin (with STATE READ permission) can see queries from all users,
   * while regular user (without STATE READ) can only see their own queries.
   */
  @Test
  @Timeout(60)
  public void test_getRunningQueries_authorization()
  {
    final String adminQueryId = "admin-query-" + UUID.randomUUID();
    final String regularUserQueryId = "regular-query-" + UUID.randomUUID();

    // Run a query as admin
    runSqlWithClient(
        StringUtils.format(
            "SET engine = 'msq-dart';\n"
            + "SET sqlQueryId = '%s';\n"
            + "SELECT COUNT(*) FROM \"%s\"",
            adminQueryId,
            ingestedDataSource
        ),
        adminClient
    );

    // Run a query as regular user
    runSqlWithClient(
        StringUtils.format(
            "SET engine = 'msq-dart';\n"
            + "SET sqlQueryId = '%s';\n"
            + "SELECT COUNT(*) FROM \"%s\"",
            regularUserQueryId,
            ingestedDataSource
        ),
        regularUserClient
    );

    // Admin should see both queries
    final GetQueriesResponse adminResponse = getRunningQueriesWithClient(adminClient);
    Assertions.assertNotNull(adminResponse);

    final List<String> adminVisibleSqlQueryIds = getSqlQueryIds(adminResponse);
    Assertions.assertTrue(adminVisibleSqlQueryIds.contains(adminQueryId));
    Assertions.assertTrue(adminVisibleSqlQueryIds.contains(regularUserQueryId));

    // Admin can get either query report
    Assertions.assertNotNull(getReportWithClient(adminQueryId, adminClient));
    Assertions.assertNotNull(getReportWithClient(regularUserQueryId, adminClient));

    // Regular user should only see their own query
    final GetQueriesResponse regularUserResponse = getRunningQueriesWithClient(regularUserClient);
    Assertions.assertNotNull(regularUserResponse);

    final List<String> regularUserVisibleSqlQueryIds = getSqlQueryIds(regularUserResponse);
    Assertions.assertFalse(regularUserVisibleSqlQueryIds.contains(adminQueryId));
    Assertions.assertTrue(regularUserVisibleSqlQueryIds.contains(regularUserQueryId));

    // Regular user can get only their own query report
    final RuntimeException e = Assertions.assertThrows(
        RuntimeException.class,
        () -> getReportWithClient(adminQueryId, regularUserClient)
    );
    MatcherAssert.assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("404 Not Found")));
    Assertions.assertNotNull(getReportWithClient(regularUserQueryId, regularUserClient));
  }

  /**
   * Test that admin (with STATE READ permission) can see queries from all users in sys.queries,
   * while regular user (without STATE READ) can only see their own queries.
   */
  @Test
  @Timeout(60)
  public void test_sysQueries_authorization() throws IOException
  {
    final String adminQueryId = "admin-sys-query-" + UUID.randomUUID();
    final String regularUserQueryId = "regular-sys-query-" + UUID.randomUUID();

    // Run a query as admin
    runSqlWithClient(
        StringUtils.format(
            "SET engine = 'msq-dart';\n"
            + "SET sqlQueryId = '%s';\n"
            + "SELECT COUNT(*) FROM \"%s\"",
            adminQueryId,
            ingestedDataSource
        ),
        adminClient
    );

    // Run a query as regular user
    runSqlWithClient(
        StringUtils.format(
            "SET engine = 'msq-dart';\n"
            + "SET sqlQueryId = '%s';\n"
            + "SELECT COUNT(*) FROM \"%s\"",
            regularUserQueryId,
            ingestedDataSource
        ),
        regularUserClient
    );

    // Admin queries sys.queries and should see both queries
    final List<String> adminVisibleSqlQueryIds = getSqlQueryIdsFromSysQueries(adminClient);
    Assertions.assertTrue(adminVisibleSqlQueryIds.contains(adminQueryId));
    Assertions.assertTrue(adminVisibleSqlQueryIds.contains(regularUserQueryId));

    // Regular user queries sys.queries and should only see their own query
    final List<String> regularUserVisibleSqlQueryIds = getSqlQueryIdsFromSysQueries(regularUserClient);
    Assertions.assertFalse(regularUserVisibleSqlQueryIds.contains(adminQueryId));
    Assertions.assertTrue(regularUserVisibleSqlQueryIds.contains(regularUserQueryId));
  }

  /**
   * Extracts SQL query IDs from a {@link GetQueriesResponse} for Dart queries only.
   */
  private static List<String> getSqlQueryIds(GetQueriesResponse response)
  {
    return response.getQueries().stream()
                   .filter(q -> q instanceof DartQueryInfo)
                   .map(q -> ((DartQueryInfo) q).getSqlQueryId())
                   .collect(Collectors.toList());
  }

  /**
   * Queries sys.queries and extracts SQL query IDs for Dart queries.
   * The info column contains JSON with the sqlQueryId field.
   */
  private List<String> getSqlQueryIdsFromSysQueries(HttpClient httpClient) throws IOException
  {
    final String sysQueriesResult = runSqlWithClient(
        "SELECT info FROM sys.queries WHERE engine = 'msq-dart'",
        httpClient
    ).trim();

    if (sysQueriesResult.isEmpty()) {
      return List.of();
    }

    final List<String> sqlQueryIds = new ArrayList<>();
    for (String line : sysQueriesResult.split("\n")) {
      // Each line is a CSV row with the info JSON field; parse it to handle proper CSV escaping
      final String[] csvFields = CsvInputFormat.createOpenCsvParser().parseLine(line);
      final DartQueryInfo info = (DartQueryInfo) broker1.bindings().jsonMapper().readValue(
          csvFields[0],
          QueryInfo.class
      );
      if (info.getSqlQueryId() == null) {
        throw DruidException.defensive("Missing sqlQueryId in info[%s]", info);
      }
      sqlQueryIds.add(info.getSqlQueryId());
    }
    return sqlQueryIds;
  }

  /**
   * Polls the report API on {@link #broker1} until a report is available.
   */
  private GetQueryReportResponse waitForReport(String sqlQueryId)
  {
    final long timeout = 30_000;
    final long deadline = System.currentTimeMillis() + timeout;
    while (System.currentTimeMillis() < deadline) {
      final GetQueryReportResponse report = msqApis.getDartQueryReport(sqlQueryId, broker1);
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

  /**
   * Gets running queries from {@link #broker1} using the provided HTTP client for authentication.
   */
  private GetQueryReportResponse getReportWithClient(String queryId, HttpClient httpClient)
  {
    final String brokerUrl = getBrokerUrl(broker1);
    final String url = StringUtils.format(
        "%s/druid/v2/sql/queries/%s/reports",
        brokerUrl,
        StringUtils.urlEncode(queryId)
    );

    final StatusResponseHolder response = HttpUtil.makeRequest(
        httpClient,
        HttpMethod.GET,
        url,
        null,
        HttpResponseStatus.OK
    );

    try {
      return broker1.bindings().jsonMapper().readValue(response.getContent(), GetQueryReportResponse.class);
    }
    catch (JsonProcessingException e) {
      throw DruidException.defensive(e, "Failed to parse GetQueryReportResponse");
    }
  }

  /**
   * Gets running queries from {@link #broker1} using the provided HTTP client for authentication.
   */
  private GetQueriesResponse getRunningQueriesWithClient(HttpClient httpClient)
  {
    final String brokerUrl = getBrokerUrl(broker1);
    final String url = brokerUrl + "/druid/v2/sql/queries?includeComplete";

    final StatusResponseHolder response = HttpUtil.makeRequest(
        httpClient,
        HttpMethod.GET,
        url,
        null,
        HttpResponseStatus.OK
    );

    try {
      return broker1.bindings().jsonMapper().readValue(response.getContent(), GetQueriesResponse.class);
    }
    catch (JsonProcessingException e) {
      throw DruidException.defensive(e, "Failed to parse GetQueriesResponse");
    }
  }

  /**
   * Submits a SQL query to {@link #broker1} using the provided HTTP client for authentication.
   */
  public String runSqlWithClient(
      String sql,
      HttpClient httpClient
  )
  {
    final ClientSqlQuery query = new ClientSqlQuery(
        sql,
        ResultFormat.CSV.name(),
        false,
        false,
        false,
        Map.of(),
        null
    );

    final String brokerUrl = getBrokerUrl(broker1);
    final StatusResponseHolder response = HttpUtil.makeRequest(
        httpClient,
        HttpMethod.POST,
        brokerUrl + "/druid/v2/sql",
        query,
        HttpResponseStatus.OK
    );
    return response.getContent();
  }

  private String getBrokerUrl(EmbeddedBroker broker)
  {
    return broker.bindings().selfNode().getUriToUse().toString();
  }
}
