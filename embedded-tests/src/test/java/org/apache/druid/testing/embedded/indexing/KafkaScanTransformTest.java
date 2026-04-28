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

package org.apache.druid.testing.embedded.indexing;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpecBuilder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.Druids;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.metadata.Metric;
import org.apache.druid.segment.transform.ScanTransformSpec;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Verifies ScanTransform during Kafka ingestion:
 * <ul>
 *   <li>Unnests both "tags" (string array) and "services" (object array) via nested UnnestDataSources</li>
 *   <li>Computes derived columns via virtual columns (upper case, string concat)</li>
 *   <li>All in a single scan query — demonstrates unnest + expression transforms combined</li>
 * </ul>
 */
public class KafkaScanTransformTest extends EmbeddedClusterTestBase
{
  /**
   * alice: 2 tags x 2 services = 4, bob: 1 tag x 3 services = 3 = 7 unnested rows
   * carol (null arrays) and dave (missing columns) produce 0 rows (matching native CROSS JOIN UNNEST semantics)
   * total: 7
   */
  private static final int EXPECTED_ROWS = 7;

  private final KafkaResource kafka = new KafkaResource();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  private String topic;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");

    indexer.setServerMemory(300_000_000)
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
           .addProperty("druid.processing.numThreads", "2")
           .addProperty("druid.worker.capacity", "2");

    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .addExtension(KafkaIndexTaskModule.class)
        .addResource(kafka)
        .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
        .useLatchableEmitter()
        .useDefaultTimeoutForLatchableEmitter(30)
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(broker)
        .addServer(indexer);
  }

  @Override
  protected void refreshDatasourceName()
  {
    // Do not refresh — datasource is set once in setupAll
  }

  @BeforeAll
  void setupAll() throws JsonProcessingException
  {
    topic = EmbeddedClusterApis.createTestDatasourceName();
    kafka.createTopicWithPartitions(topic, 1);

    super.refreshDatasourceName();
    submitSupervisor();
    publishTestData();

    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(EXPECTED_ROWS)
    );

    broker.latchableEmitter().waitForEvent(
        event -> event.hasMetricName(Metric.SCHEMA_ROW_SIGNATURE_COLUMN_COUNT)
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );
  }

  private void submitSupervisor()
  {
    final ScanTransformSpec transformSpec = new ScanTransformSpec(
        Druids.newScanQueryBuilder()
              .dataSource(UnnestDataSource.create(
                  UnnestDataSource.create(
                      new TableDataSource("__input__"),
                      new ExpressionVirtualColumn("tag", "\"tags\"", ColumnType.STRING, ExprMacroTable.nil()),
                      null
                  ),
                  new ExpressionVirtualColumn("svc", "\"services\"", ColumnType.NESTED_DATA, ExprMacroTable.nil()),
                  null
              ))
              .virtualColumns(
                  new ExpressionVirtualColumn(
                      "upper_user",
                      "upper(\"user\")",
                      ColumnType.STRING,
                      ExprMacroTable.nil()
                  ),
                  new ExpressionVirtualColumn(
                      "user_tag",
                      "concat(\"user\", '_', \"tag\")",
                      ColumnType.STRING,
                      ExprMacroTable.nil()
                  )
              )
              .eternityInterval()
              .columns((List<String>) null)
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .build()
    );

    final KafkaSupervisorSpec spec = new KafkaSupervisorSpecBuilder()
        .withDataSchema(
            schema -> schema
                .withTimestamp(new TimestampSpec("__time", "auto", null))
                .withGranularity(new UniformGranularitySpec(Granularities.DAY, null, null))
                .withDimensions(DimensionsSpec.builder().useSchemaDiscovery(true).build())
                .withAggregators(new LongSumAggregatorFactory("total_bytes", "bytes_sent"))
                .withTransform(transformSpec)
        )
        .withIoConfig(
            ioConfig -> ioConfig
                .withJsonInputFormat()
                .withTaskCount(1)
                .withTaskDuration(Period.hours(1))
                .withConsumerProperties(kafka.consumerProperties())
                .withStartDelay(Period.millis(10))
                .withSupervisorRunPeriod(Period.millis(500))
                .withUseEarliestSequenceNumber(true)
                .withCompletionTimeout(Period.seconds(5))
        )
        .build(dataSource, topic);

    Assertions.assertEquals(
        dataSource,
        cluster.callApi().postSupervisor(spec)
    );
  }

  private void publishTestData() throws JsonProcessingException
  {
    // alice: 2 tags x 2 services = 4 rows
    // bob: 1 tag x 3 services = 3 rows
    // carol: null tags x null services => 0 rows (CROSS JOIN UNNEST semantics)
    // dave: missing tags & services columns => 0 rows (CROSS JOIN UNNEST semantics)
    // total: 7 rows
    final List<Map<String, Object>> records = new ArrayList<>();
    // bytes_sent is a metric input (not a dimension). It must survive the scan transform so the
    // LongSumAggregatorFactory on total_bytes reads the real value, not zero.
    records.add(Map.of(
        "__time", "2024-01-01T00:00:00Z",
        "user", "alice",
        "tags", List.of("sports", "news"),
        "services", List.of(
            Map.of("type", "web", "dc", "us-east1"),
            Map.of("type", "api", "dc", "us-west2")
        ),
        "bytes_sent", 100
    ));
    records.add(Map.of(
        "__time", "2024-01-01T00:01:00Z",
        "user", "bob",
        "tags", List.of("music"),
        "services", List.of(
            Map.of("type", "cdn", "dc", "eu-west1"),
            Map.of("type", "cache", "dc", "eu-west1"),
            Map.of("type", "db", "dc", "us-east1")
        ),
        "bytes_sent", 50
    ));

    // carol: explicit null values for both array columns
    final HashMap<String, Object> carolRecord = new HashMap<>();
    carolRecord.put("__time", "2024-01-01T00:02:00Z");
    carolRecord.put("user", "carol");
    carolRecord.put("tags", null);
    carolRecord.put("services", null);
    records.add(carolRecord);

    // dave: columns not present at all
    records.add(Map.of(
        "__time", "2024-01-01T00:03:00Z",
        "user", "dave"
    ));

    final List<byte[]> recordBytes = new ArrayList<>();
    for (Map<String, Object> record : records) {
      recordBytes.add(TestHelper.JSON_MAPPER.writeValueAsBytes(record));
    }
    kafka.publishRecordsToTopic(topic, recordBytes);
  }

  @Test
  @Timeout(60)
  public void test_countRows()
  {
    Assertions.assertEquals(
        String.valueOf(EXPECTED_ROWS),
        cluster.runSql(StringUtils.format("SELECT COUNT(*) FROM \"%s\"", dataSource)).trim()
    );
  }

  @Test
  @Timeout(60)
  public void test_crossJoinUnnest()
  {
    // Use GROUP BY to get deterministic, order-independent results.
    // Each user+tag pair count reflects the number of services it was crossed with.
    final String result = cluster.runSql(
        StringUtils.format(
            "SELECT \"user\", \"tag\", COUNT(*) AS cnt FROM \"%s\" GROUP BY 1, 2 ORDER BY 1, 2",
            dataSource
        )
    );
    final Set<String> actual = new TreeSet<>(List.of(result.trim().split("\n")));
    final Set<String> expected = new TreeSet<>(List.of(
        "alice,news,2",      // news x 2 services (web, api)
        "alice,sports,2",    // sports x 2 services (web, api)
        "bob,music,3"        // music x 3 services (cdn, cache, db)
        // carol and dave dropped: null/missing arrays produce 0 rows (CROSS JOIN UNNEST semantics)
    ));
    Assertions.assertEquals(expected, actual);
  }

  @Test
  @Timeout(60)
  public void test_groupByTag()
  {
    final String result = cluster.runSql(
        StringUtils.format(
            "SELECT \"tag\", COUNT(*) AS cnt FROM \"%s\" WHERE \"tag\" IS NOT NULL GROUP BY 1 ORDER BY 1",
            dataSource
        )
    );

    Assertions.assertEquals(
        "music,3\nnews,2\nsports,2",
        result.trim()
    );

    // music: 1 x 3 services = 3, news: 1 x 2 services = 2, sports: 1 x 2 services = 2
    // carol/dave have null tags so they don't appear in this grouping
    Assertions.assertEquals(
        "music,3\nnews,2\nsports,2",
        cluster.runSql(
            StringUtils.format(
                "SELECT \"tag\", COUNT(*) AS cnt FROM \"%s\" WHERE \"tag\" IS NOT NULL GROUP BY 1 ORDER BY 1",
                dataSource
            )
        )
    );
  }

  @Test
  @Timeout(60)
  public void test_groupByUser()
  {
    Assertions.assertEquals(
        "alice,4\nbob,3",
        cluster.runSql(
            StringUtils.format(
                "SELECT \"user\", COUNT(*) AS cnt FROM \"%s\" GROUP BY 1 ORDER BY 1",
                dataSource
            )
        )
    );
  }

  @Test
  @Timeout(60)
  public void test_groupByServiceType()
  {
    // Extract the "type" field from the unnested service objects using JSON_VALUE
    final String result = cluster.runSql(
        StringUtils.format(
            "SELECT JSON_VALUE(\"svc\", '$.type'), COUNT(*) AS cnt"
            + " FROM \"%s\""
            + " WHERE \"svc\" IS NOT NULL"
            + " GROUP BY 1 ORDER BY 1",
            dataSource
        )
    );
    // alice has 2 tags so each of her services appears twice (cross join)
    // bob has 1 tag so each of his services appears once
    final Set<String> actual = new TreeSet<>(List.of(result.trim().split("\n")));
    final Set<String> expected = new TreeSet<>(List.of(
        "api,2",    // alice: api x (sports, news)
        "cache,1",  // bob: cache x music
        "cdn,1",    // bob: cdn x music
        "db,1",     // bob: db x music
        "web,2"     // alice: web x (sports, news)
    ));
    Assertions.assertEquals(expected, actual);
  }

  @Test
  @Timeout(60)
  public void test_groupByServiceDc()
  {
    // Extract the "dc" field from the unnested service objects
    final String result = cluster.runSql(
        StringUtils.format(
            "SELECT JSON_VALUE(\"svc\", '$.dc'), COUNT(*) AS cnt"
            + " FROM \"%s\""
            + " WHERE \"svc\" IS NOT NULL"
            + " GROUP BY 1 ORDER BY 1",
            dataSource
        )
    );
    final Set<String> actual = new TreeSet<>(List.of(result.trim().split("\n")));
    final Set<String> expected = new TreeSet<>(List.of(
        "eu-west1,2",  // bob: cdn + cache (both eu-west1) x 1 tag
        "us-east1,3",  // alice: web(us-east1) x 2 tags + bob: db(us-east1) x 1 tag
        "us-west2,2"   // alice: api(us-west2) x 2 tags
    ));
    Assertions.assertEquals(expected, actual);
  }

  @Test
  @Timeout(60)
  public void test_upperCaseVirtualColumn()
  {
    final String result = cluster.runSql(
        StringUtils.format(
            "SELECT \"upper_user\", COUNT(*) AS cnt FROM \"%s\" GROUP BY 1 ORDER BY 1",
            dataSource
        )
    );
    Assertions.assertEquals(
        "ALICE,4\nBOB,3",
        result.trim()
    );
  }

  @Test
  @Timeout(60)
  public void test_concatVirtualColumn()
  {
    // user_tag = concat(user, '_', tag) — computed at ingest time via scan query virtual column
    final String result = cluster.runSql(
        StringUtils.format(
            "SELECT \"user_tag\", COUNT(*) AS cnt"
            + " FROM \"%s\""
            + " WHERE \"tag\" IS NOT NULL"
            + " GROUP BY 1 ORDER BY 1",
            dataSource
        )
    );
    final Set<String> actual = new TreeSet<>(List.of(result.trim().split("\n")));
    final Set<String> expected = new TreeSet<>(List.of(
        "alice_news,2",     // alice_news x 2 services
        "alice_sports,2",   // alice_sports x 2 services
        "bob_music,3"       // bob_music x 3 services
    ));
    Assertions.assertEquals(expected, actual);
  }

  @Test
  @Timeout(60)
  public void test_filterByServiceType()
  {
    // Filter to only rows where the service type is "web"
    final String result = cluster.runSql(
        StringUtils.format(
            "SELECT \"user\", \"tag\", JSON_VALUE(\"svc\", '$.type'), JSON_VALUE(\"svc\", '$.dc')"
            + " FROM \"%s\""
            + " WHERE JSON_VALUE(\"svc\", '$.type') = 'web'",
            dataSource
        )
    );
    final Set<String> actual = new TreeSet<>(List.of(result.trim().split("\n")));
    final Set<String> expected = new TreeSet<>(List.of(
        "alice,news,web,us-east1",
        "alice,sports,web,us-east1"
    ));
    Assertions.assertEquals(expected, actual);
  }

  @Test
  @Timeout(60)
  public void test_metricInputSurvivesUnnestExpansion()
  {
    // bytes_sent is a metric input, not a dimension. It must be preserved on every expanded row
    // after the scan transform, otherwise LongSum aggregates to 0.
    // alice: 4 expanded rows x 100 = 400, bob: 3 expanded rows x 50 = 150, total = 550.
    Assertions.assertEquals(
        "550",
        cluster.runSql(StringUtils.format("SELECT SUM(\"total_bytes\") FROM \"%s\"", dataSource)).trim()
    );

    // Per-user sums confirm the metric value is preserved unchanged on each unnested row.
    Assertions.assertEquals(
        "alice,400\nbob,150",
        cluster.runSql(
            StringUtils.format(
                "SELECT \"user\", SUM(\"total_bytes\") FROM \"%s\" GROUP BY 1 ORDER BY 1",
                dataSource
            )
        ).trim()
    );
  }
}
