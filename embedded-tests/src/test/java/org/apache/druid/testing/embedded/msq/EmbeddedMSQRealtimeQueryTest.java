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
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.msq.dart.guice.DartControllerMemoryManagementModule;
import org.apache.druid.msq.dart.guice.DartControllerModule;
import org.apache.druid.msq.dart.guice.DartWorkerMemoryManagementModule;
import org.apache.druid.msq.dart.guice.DartWorkerModule;
import org.apache.druid.msq.guice.IndexerMemoryManagementModule;
import org.apache.druid.msq.guice.MSQDurableStorageModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.guice.MSQSqlModule;
import org.apache.druid.msq.guice.SqlTaskModule;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.joda.time.Period;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Embedded test to ingest {@link TestIndex#getMMappedWikipediaIndex()} into Kafka tasks, then query
 * those tasks with MSQ.
 */
public class EmbeddedMSQRealtimeQueryTest extends EmbeddedClusterTestBase
{
  private static final Period TASK_DURATION = Period.hours(1);
  private static final int TASK_COUNT = 2;
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

  private KafkaResource kafka;
  private String topic;
  private EmbeddedMSQApis msqApis;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    kafka = new KafkaResource();

    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");

    overlord.addProperty("druid.manager.segments.useIncrementalCache", "always")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s");

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

    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .addExtensions(
            KafkaIndexTaskModule.class,
            DartControllerModule.class,
            DartWorkerModule.class,
            DartControllerMemoryManagementModule.class,
            DartWorkerMemoryManagementModule.class,
            IndexerMemoryManagementModule.class,
            MSQDurableStorageModule.class,
            MSQIndexingModule.class,
            MSQSqlModule.class,
            SqlTaskModule.class
        )
        .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
        .addCommonProperty("druid.msq.dart.enabled", "true")
        .useLatchableEmitter()
        .addResource(kafka)
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(router);
  }

  @BeforeAll
  @Override
  protected void setup() throws Exception
  {
    super.setup();

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
  }

  @BeforeEach
  void setUpEach()
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
    topic = dataSource = EmbeddedClusterApis.createTestDatasourceName();

    // Create Kafka topic.
    kafka.createTopicWithPartitions(topic, 2);

    // Submit a supervisor.
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor();
    final Map<String, String> startSupervisorResult =
        cluster.callApi().onLeaderOverlord(o -> o.postSupervisor(kafkaSupervisorSpec));
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    // Send data to Kafka.
    final QueryableIndexCursorFactory wikiCursorFactory =
        new QueryableIndexCursorFactory(TestIndex.getMMappedWikipediaIndex());
    final RowSignature wikiSignature = wikiCursorFactory.getRowSignature();
    kafka.produceRecordsToTopic(
        FrameTestUtil.readRowsFromCursorFactory(wikiCursorFactory)
                     .map(row -> {
                       final Map<String, Object> rowMap = new LinkedHashMap<>();
                       for (int i = 0; i < row.size(); i++) {
                         rowMap.put(wikiSignature.getColumnName(i), row.get(i));
                       }
                       try {
                         return new ProducerRecord<>(
                             topic,
                             ByteArrays.EMPTY_ARRAY,
                             TestHelper.JSON_MAPPER.writeValueAsBytes(rowMap)
                         );
                       }
                       catch (JsonProcessingException e) {
                         throw new RuntimeException(e);
                       }
                     })
                     .toList()
    );

    // Wait for it to be loaded.
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, Collections.singletonList(dataSource)),
        agg -> agg.hasSumAtLeast(totalRows)
    );
  }

  @AfterEach
  void tearDownEach() throws ExecutionException, InterruptedException, IOException
  {
    final Map<String, String> terminateSupervisorResult =
        cluster.callApi().onLeaderOverlord(o -> o.terminateSupervisor(dataSource));
    Assertions.assertEquals(Map.of("id", dataSource), terminateSupervisorResult);

    // Cancel all running tasks, so we don't need to wait for them to hand off their segments.
    try (final CloseableIterator<TaskStatusPlus> it = cluster.leaderOverlord().taskStatuses(null, null, null).get()) {
      while (it.hasNext()) {
        cluster.leaderOverlord().cancelTask(it.next().getId());
      }
    }

    kafka.deleteTopic(topic);
  }

  @Test
  @Timeout(60)
  public void test_selectCount_task_default()
  {
    final String sql = StringUtils.format("SELECT COUNT(*) FROM \"%s\"", dataSource);
    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

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

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

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
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
            "Unknown InputNumberDataSource datasource with number[1]. Queries with realtime sources "
            + "cannot join results with stage outputs. Use sortMerge join instead by setting [sqlJoinAlgorithm]."))
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
    cluster.callApi().waitForTaskToFinish(taskId, overlord);

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
            "Unknown InputNumberDataSource datasource with number[1]. Queries with realtime sources "
            + "cannot join results with stage outputs. Use sortMerge join instead by setting [sqlJoinAlgorithm].")
                    .matches(currentStatus.getStatus().getErrorMsg())
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

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

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

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

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

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

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

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

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

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

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

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

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

  private KafkaSupervisorSpec createKafkaSupervisor()
  {
    final Period startDelay = Period.millis(10);
    final Period supervisorRunPeriod = Period.millis(500);
    final boolean useEarliestOffset = true;

    return new KafkaSupervisorSpec(
        dataSource,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("__time", "auto", null))
                  .withGranularity(new UniformGranularitySpec(Granularities.DAY, null, null))
                  .withDimensions(DimensionsSpec.builder().useSchemaDiscovery(true).build())
                  .build(),
        null,
        new KafkaSupervisorIOConfig(
            topic,
            null,
            new JsonInputFormat(null, null, null, null, null),
            null,
            TASK_COUNT,
            TASK_DURATION,
            kafka.consumerProperties(),
            null,
            null,
            null,
            startDelay,
            supervisorRunPeriod,
            useEarliestOffset,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }
}
