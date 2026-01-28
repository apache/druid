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

import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Embedded test to ingest {@link TestIndex#getMMappedWikipediaIndex()} into Kafka tasks, then query
 * those tasks with MSQ.
 */
public class EmbeddedMSQRealtimeQueryTest extends BaseRealtimeQueryTest
{
  private static final String LOOKUP_TABLE = "test_lookup";
  private static final String BULK_UPDATE_LOOKUP_PAYLOAD
      = "{\n"
        + "  \"__default\": {\n"
        + "    \"%s\": {\n"
        + "      \"version\": \"v1\",\n"
        + "      \"lookupExtractorFactory\": {\n"
        + "        \"type\": \"map\",\n"
        + "        \"map\": {\n"
        + "          \"#en.wikipedia\": \"English\",\n"
        + "          \"#fr.wikipedia\": \"French\",\n"
        + "          \"#eu.wikipedia\": \"European\",\n"
        + "          \"#ar.wikipedia\": \"Arabic\",\n"
        + "          \"#cs.wikipedia\": \"Czech\"\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  }\n"
        + "}";

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedRouter router = new EmbeddedRouter();
  private final int totalRows = TestIndex.getMMappedWikipediaIndex().getNumRows();

  private EmbeddedMSQApis msqApis;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    EmbeddedDruidCluster clusterWithKafka = super.createCluster();

    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");

    broker.addProperty("druid.msq.dart.controller.heapFraction", "0.9")
          .addProperty("druid.query.default.context.maxConcurrentStages", "1");

    historical.addProperty("druid.msq.dart.worker.heapFraction", "0.9")
              .addProperty("druid.msq.dart.worker.concurrentQueries", "1")
              .addProperty("druid.lookup.enableLookupSyncOnStartup", "true");

    indexer.setServerMemory(400_000_000) // to run 2x realtime and 2x MSQ tasks
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
           // druid.processing.numThreads must be higher than # of MSQ tasks to avoid contention, because the realtime
           // server is contacted in such a way that the processing thread is blocked
           .addProperty("druid.processing.numThreads", "3")
           .addProperty("druid.worker.capacity", "4")
           .addProperty("druid.lookup.enableLookupSyncOnStartup", "true");

    return clusterWithKafka
        .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
        .addCommonProperty("druid.msq.dart.enabled", "true")
        .useLatchableEmitter()
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(router);
  }

  @BeforeAll
  protected void setupLookups() throws Exception
  {
    // Initialize lookups
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateAllLookups(Map.of())
    );

    final String lookupPayload = StringUtils.format(
        BULK_UPDATE_LOOKUP_PAYLOAD,
        LOOKUP_TABLE
    );
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateAllLookups(EmbeddedClusterApis.deserializeJsonToMap(lookupPayload))
    );

    // Initialize the broker/data-servers later so that lookups are loaded on startup.
    cluster.addServer(broker)
           .addServer(indexer)
           .addServer(historical);
    broker.start();
    indexer.start();
    historical.start();

    msqApis = new EmbeddedMSQApis(cluster, overlord);
    submitSupervisor();
    publishToKafka(TestIndex.getMMappedWikipediaIndex());

    // Wait for it to be loaded.
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(totalRows)
    );
    broker.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("segment/schemaCache/refresh/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );
  }

  @Test
  @Timeout(60)
  public void test_selectCount_task_default()
  {
    final String sql = StringUtils.format("SELECT COUNT(*) FROM \"%s\"", dataSource);
    final MSQTaskReportPayload payload = msqApis.runTaskSqlAndGetReport(sql);

    // By default tasks do not include realtime data; count is zero.
    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        Collections.singletonList(new Object[]{0}),
        payload.getResults().getResults()
    );
  }

  @Test
  @Timeout(60)
  public void test_selectCount_task_withRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'REALTIME';\n"
        + "SELECT COUNT(*) FROM \"%s\"",
        dataSource
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSqlAndGetReport(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        Collections.singletonList(new Object[]{totalRows}),
        payload.getResults().getResults()
    );
  }

  @Test
  @Timeout(60)
  public void test_selectCount_dart_default()
  {
    final String sql = StringUtils.format("SELECT COUNT(*) FROM \"%s\"", dataSource);
    final long selectedCount = Long.parseLong(msqApis.runDartSql(sql));

    // By default Dart includes realtime data.
    Assertions.assertEquals(totalRows, selectedCount);
  }

  @Test
  @Timeout(60)
  public void test_selectCount_dart_noRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'NONE';\n"
        + "SELECT COUNT(*) FROM \"%s\"",
        dataSource
    );

    final long selectedCount = Long.parseLong(msqApis.runDartSql(sql));
    Assertions.assertEquals(0, selectedCount);
  }

  @Test
  @Timeout(60)
  public void test_selectBroadcastJoin_dart()
  {
    final String sql = "SELECT COUNT(*) FROM \"%s\"\n"
                       + "WHERE countryName IN (\n"
                       + "  SELECT countryName\n"
                       + "  FROM \"%s\"\n"
                       + "  WHERE countryName IS NOT NULL\n"
                       + "  GROUP BY 1\n"
                       + "  ORDER BY COUNT(*) DESC\n"
                       + "  LIMIT 1\n"
                       + ")";

    MatcherAssert.assertThat(
        Assertions.assertThrows(
            RuntimeException.class,
            () -> msqApis.runDartSql(sql, dataSource, dataSource)
        ),
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString(
                "Cannot handle stage with multiple sources while querying realtime data. If using broadcast "
                + "joins, try setting[sqlJoinAlgorithm] to[sortMerge] in your query context."
            )
        )
    );
  }

  @Test
  @Timeout(60)
  public void test_selectBroadcastJoin_task_withRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'REALTIME';\n"
        + "SELECT COUNT(*) FROM \"%s\"\n"
        + "WHERE countryName IN (\n"
        + "  SELECT countryName\n"
        + "  FROM \"%s\"\n"
        + "  WHERE countryName IS NOT NULL\n"
        + "  GROUP BY 1\n"
        + "  ORDER BY COUNT(*) DESC\n"
        + "  LIMIT 1\n"
        + ")",
        dataSource,
        dataSource
    );

    SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);

    String taskId = taskStatus.getTaskId();
    cluster.callApi().waitForTaskToFinish(taskId, overlord.latchableEmitter());

    final TaskStatusResponse currentStatus = cluster.callApi().onLeaderOverlord(
        o -> o.taskStatus(taskId)
    );
    Assertions.assertNotNull(currentStatus.getStatus());
    Assertions.assertEquals(
        TaskState.FAILED,
        currentStatus.getStatus().getStatusCode(),
        StringUtils.format("Task[%s] has unexpected status", taskId)
    );

    Assertions.assertTrue(
        CoreMatchers.containsString(
            "Cannot handle stage with multiple sources while querying realtime data. If using broadcast "
            + "joins, try setting[sqlJoinAlgorithm] to[sortMerge] in your query context."
        ).matches(currentStatus.getStatus().getErrorMsg())
    );
  }

  @Test
  @Timeout(60)
  public void test_selectSortMergeJoin_task_withRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'REALTIME';"
        + "SET sqlJoinAlgorithm = 'sortMerge';\n"
        + "SELECT COUNT(*) FROM \"%s\"\n"
        + "WHERE countryName IN (\n"
        + "  SELECT countryName\n"
        + "  FROM \"%s\"\n"
        + "  WHERE countryName IS NOT NULL\n"
        + "  GROUP BY 1\n"
        + "  ORDER BY COUNT(*) DESC\n"
        + "  LIMIT 1\n"
        + ")",
        dataSource,
        dataSource
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSqlAndGetReport(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        Collections.singletonList(new Object[]{528}),
        payload.getResults().getResults()
    );
  }

  @Test
  @Timeout(60)
  public void test_selectSortMergeJoin_dart()
  {
    final long selectedCount = Long.parseLong(
        msqApis.runDartSql(
            "SET sqlJoinAlgorithm = 'sortMerge';\n"
            + "SELECT COUNT(*) FROM \"%s\"\n"
            + "WHERE countryName IN (\n"
            + "  SELECT countryName\n"
            + "  FROM \"%s\"\n"
            + "  WHERE countryName IS NOT NULL\n"
            + "  GROUP BY 1\n"
            + "  ORDER BY COUNT(*) DESC\n"
            + "  LIMIT 1\n"
            + ")",
            dataSource,
            dataSource
        )
    );

    Assertions.assertEquals(528, selectedCount);
  }

  @Test
  @Timeout(60)
  public void test_selectJoinwithLookup_task_withRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'REALTIME';\n"
        + "SELECT \n"
        + " l.v AS newName, \n"
        + " SUM(w.\"added\") AS total\n"
        + "FROM \"%s\" w INNER JOIN lookup.%s l ON w.\"channel\" = l.k\n"
        + "GROUP BY 1\n"
        + "ORDER BY 2 DESC\n",
        dataSource,
        LOOKUP_TABLE
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSqlAndGetReport(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        List.of(
            new Object[]{"English", 3045299},
            new Object[]{"French", 642555},
            new Object[]{"Arabic", 153605},
            new Object[]{"Czech", 132768},
            new Object[]{"European", 6690}
        ),
        payload.getResults().getResults()
    );
  }

  @Test
  @Timeout(60)
  public void test_selectJoinwithLookup_dart()
  {
    final String sql = StringUtils.format(
        "SELECT \n"
        + " l.v AS newName, \n"
        + " SUM(w.\"added\") AS total\n"
        + "FROM \"%s\" w INNER JOIN lookup.%s l ON w.\"channel\" = l.k\n"
        + "GROUP BY 1\n"
        + "ORDER BY 2 DESC\n",
        dataSource,
        LOOKUP_TABLE
    );

    final String result = msqApis.runDartSql(sql);

    Assertions.assertEquals(
        "English,3045299\n"
        + "French,642555\n"
        + "Arabic,153605\n"
        + "Czech,132768\n"
        + "European,6690",
        result
    );
  }

  @Test
  @Timeout(60)
  public void test_selectJoinWithConcatVirtualDimension_task_withRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'REALTIME';\n"
        + "SELECT\n"
        + "  \"channel\",\n"
        + "  \"countryIsoCode\",\n"
        + "  CONCAT(w.\"cityName\", ': ', l.v),\n"
        + "  \"user\"\n"
        + "FROM %s w\n"
        + "  INNER JOIN lookup.%s l ON w.\"channel\" = l.k\n"
        + "WHERE\n"
        + "  w.\"cityName\" IS NOT NULL\n"
        + "  AND \"added\" > 1000 AND \"delta\" > 5000\n"
        + "ORDER BY 3 DESC\n",
        dataSource,
        LOOKUP_TABLE
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSqlAndGetReport(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        List.of(
            new Object[]{"#en.wikipedia", "GB", "London: English", "78.145.31.93"},
            new Object[]{"#ar.wikipedia", "AE", "Dubai: Arabic", "86.98.5.51"},
            new Object[]{"#en.wikipedia", "IN", "Bhopal: English", "14.139.241.50"}
        ),
        payload.getResults().getResults()
    );
  }

  @Test
  @Timeout(60)
  public void test_selectJoinWithConcatVirtualDimension_dart()
  {
    final String sql = StringUtils.format(
        "SELECT\n"
        + "  \"channel\",\n"
        + "  \"countryIsoCode\",\n"
        + "  CONCAT(w.\"cityName\", ': ', l.v),\n"
        + "  \"user\"\n"
        + "FROM %s w\n"
        + "  INNER JOIN lookup.%s l ON w.\"channel\" = l.k\n"
        + "WHERE\n"
        + "  w.\"cityName\" IS NOT NULL\n"
        + "  AND \"added\" > 1000 AND \"delta\" > 5000\n"
        + "ORDER BY 3 DESC\n",
        dataSource,
        LOOKUP_TABLE
    );

    final String results = msqApis.runDartSql(sql);

    Assertions.assertEquals(
        "#en.wikipedia,GB,London: English,78.145.31.93\n"
        + "#ar.wikipedia,AE,Dubai: Arabic,86.98.5.51\n"
        + "#en.wikipedia,IN,Bhopal: English,14.139.241.50",
        results
    );
  }

  @Test
  @Timeout(60)
  public void test_scanWithFilter_task_withRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'REALTIME';\n"
        + "SELECT \"channel\", \"page\", \"user\", \"deleted\"\n"
        + "FROM \"%s\"\n"
        + "WHERE \"cityName\" = 'Sydney' AND \"delta\" > 10",
        dataSource
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSqlAndGetReport(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        List.of(
            new Object[]{"#en.wikipedia", "Coca-Cola formula", "124.169.17.234", 0},
            new Object[]{"#en.wikipedia", "List of Harry Potter characters", "121.211.82.121", 0}
        ),
        payload.getResults().getResults()
    );
  }


  @Test
  @Timeout(60)
  public void test_scanWithFilter_dart()
  {
    final String sql = StringUtils.format(
        "SELECT \"channel\", \"page\", \"user\", \"deleted\"\n"
        + "FROM \"%s\"\n"
        + "WHERE \"cityName\" = 'Sydney' AND \"delta\" > 10",
        dataSource
    );

    final String result = msqApis.runDartSql(sql);

    Assertions.assertEquals(
        "#en.wikipedia,Coca-Cola formula,124.169.17.234,0\n#en.wikipedia,List of Harry Potter characters,121.211.82.121,0",
        result
    );
  }

  @Test
  @Timeout(60)
  public void test_groupByWithFilter_task_withRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'REALTIME';\n"
        + "SELECT \"channel\", COUNT(*)\n"
        + "FROM \"%s\"\n"
        + "WHERE \"countryName\" = 'Australia'\n"
        + "GROUP BY 1\n"
        + "ORDER BY 1 DESC",
        dataSource
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSqlAndGetReport(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        List.of(
            new Object[]{"#en.wikipedia", 63},
            new Object[]{"#de.wikipedia", 2}
        ),
        payload.getResults().getResults()
    );
  }

  @Test
  @Timeout(60)
  public void test_groupByWithFilter_dart()
  {
    final String sql = StringUtils.format(
        "SELECT \"channel\", COUNT(*)\n"
        + "FROM \"%s\"\n"
        + "WHERE \"countryName\" = 'Australia'\n"
        + "GROUP BY 1\n"
        + "ORDER BY 1 DESC",
        dataSource
    );

    final String result = msqApis.runDartSql(sql);

    Assertions.assertEquals("#en.wikipedia,63\n#de.wikipedia,2", result);
  }

  @Test
  @Timeout(60)
  public void test_scanWithFilterAfterJoin_task_withRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'REALTIME';\n"
        + "SELECT \n"
        + "  \"page\", \n"
        + "  \"user\", \n"
        + "  \"added\"\n"
        + "FROM %s w\n"
        + "  INNER JOIN lookup.%s l ON w.\"channel\" = l.k\n"
        + "WHERE CONCAT(w.\"cityName\", ': ', l.v) = 'London: English' AND \"comment\" IN ('/* Works */', '/* Early life */')\n",
        dataSource,
        LOOKUP_TABLE
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSqlAndGetReport(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        List.of(
            new Object[]{"Andy Wilman", "109.156.217.121", 0},
            new Object[]{"Angharad Rees", "89.240.46.182", 578},
            new Object[]{"Chazz Palminteri", "81.178.229.60", 10}
        ),
        payload.getResults().getResults()
    );
  }

  @Test
  @Timeout(60)
  public void test_scanWithFilterAfterJoin_dart()
  {
    final String sql = StringUtils.format(
        "SELECT \n"
        + "  \"page\", \n"
        + "  \"user\", \n"
        + "  \"added\"\n"
        + "FROM %s w\n"
        + "  INNER JOIN lookup.%s l ON w.\"channel\" = l.k\n"
        + "WHERE CONCAT(w.\"cityName\", ': ', l.v) = 'London: English' AND \"comment\" IN ('/* Works */', '/* Early life */')\n",
        dataSource,
        LOOKUP_TABLE
    );

    final String result = msqApis.runDartSql(sql);

    Assertions.assertEquals(
        "Andy Wilman,109.156.217.121,0\nAngharad Rees,89.240.46.182,578\nChazz Palminteri,81.178.229.60,10",
        result
    );
  }
}
