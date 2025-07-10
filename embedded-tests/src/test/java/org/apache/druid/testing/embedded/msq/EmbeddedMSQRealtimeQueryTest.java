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
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.emitter.kafka.KafkaEmitter;
import org.apache.druid.frame.testutil.FrameTestUtil;
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
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Embedded test to emit cluster metrics using a {@link KafkaEmitter} and then
 * ingest them back into the cluster with a {@code KafkaSupervisor}.
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
        + "          \"#en.wikipedia\": \"A\",\n"
        + "          \"#fr.wikipedia\": \"B\",\n"
        + "          \"#eu.wikipedia\": \"C\",\n"
        + "          \"#ar.wikipedia\": \"D\",\n"
        + "          \"#cs.wikipedia\": \"E\"\n"
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
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();

    kafka = new KafkaResource();

    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");

    overlord.addProperty("druid.manager.segments.useIncrementalCache", "always")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s");

    broker.addProperty("druid.msq.dart.controller.heapFraction", "0.9")
          .addProperty("druid.query.default.context.maxConcurrentStages", "1");

    historical.addProperty("druid.msq.dart.worker.heapFraction", "0.9")
              .addProperty("druid.msq.dart.worker.concurrentQueries", "1")
              .addProperty("druid.lookup.enableLookupSyncOnStartup", "true");

    indexer.setServerMemory(300_000_000) // to run 2x realtime and 2x MSQ tasks
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
           // druid.processing.numThreads must be higher than # of MSQ tasks to avoid contention, because the realtime
           // server is contacted in such a way that the processing thread is blocked
           .addProperty("druid.processing.numThreads", "3")
           .addProperty("druid.worker.capacity", "4")
           .addProperty("druid.lookup.enableLookupSyncOnStartup", "true");

    cluster.addExtension(KafkaIndexTaskModule.class)
           .addExtension(DartControllerModule.class)
           .addExtension(DartWorkerModule.class)
           .addExtension(DartControllerMemoryManagementModule.class)
           .addExtension(DartControllerModule.class)
           .addExtension(DartWorkerMemoryManagementModule.class)
           .addExtension(DartWorkerModule.class)
           .addExtension(IndexerMemoryManagementModule.class)
           .addExtension(MSQDurableStorageModule.class)
           .addExtension(MSQIndexingModule.class)
           .addExtension(MSQSqlModule.class)
           .addExtension(SqlTaskModule.class)
           .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
           .addCommonProperty("druid.msq.dart.enabled", "true")
           .useLatchableEmitter()
           .addResource(kafka);

    // Initialize the indexers and brokers later.
    cluster.addServer(coordinator)
           .addServer(overlord)
//           .addServer(indexer)
//           .addServer(broker)
//           .addServer(historical)
           .addServer(router);

    return cluster;
  }

  @BeforeEach
  void setUpEach() throws Exception
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
    topic = dataSource = EmbeddedClusterApis.createTestDatasourceName();

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

    // Initialize the broker/data-servers later so that lookups are already loaded.
    cluster.addServer(broker);
    broker.start();
    cluster.addServer(indexer);
    indexer.start();
    cluster.addServer(historical);
    indexer.start();

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
  @Disabled // Test does not currently pass, see https://github.com/apache/druid/issues/18198
  public void test_selectBroadcastJoin_dart()
  {
    final long selectedCount = Long.parseLong(
        msqApis.runDartSql(
            "SELECT COUNT(*) FROM \"%s\"\n"
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
  @Disabled // Test does not currently pass, see https://github.com/apache/druid/issues/18198
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

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        Collections.singletonList(new Object[]{528}),
        payload.getResults().getResults()
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
  @Disabled
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
            new Object[]{"A", 3045299},
            new Object[]{"B", 642555},
            new Object[]{"D", 153605},
            new Object[]{"E", 132768},
            new Object[]{"C", 6690}
        ),
        payload.getResults().getResults()
    );
  }

  @Test
  @Timeout(60)
  @Disabled
  public void test_selectJoinWithUnnest_task_withRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'REALTIME';\n"
        + "SELECT *\n"
        + "FROM \"%s\"\n",
        dataSource
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        List.of(
            new Object[]{"#en.wikipedia", 3045299},
            new Object[]{"#fr.wikipedia", 642555}
        ),
        payload.getResults().getResults()
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
