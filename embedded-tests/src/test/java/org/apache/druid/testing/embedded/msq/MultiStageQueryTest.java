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

import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.http.GetQueryReportResponse;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class MultiStageQueryTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker =
      new EmbeddedBroker().setServerMemory(200_000_000)
                          .addProperty("druid.msq.dart.controller.maxRetainedReportCount", "10")
                          .addProperty("druid.query.default.context.maxConcurrentStages", "1")
                          .addProperty("druid.sql.planner.enableSysQueriesTable", "true");
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "2");
  private final MSQExportDirectory exportDirectory = new MSQExportDirectory();

  private EmbeddedMSQApis msqApis;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addCommonProperty("druid.msq.dart.enabled", "true")
        .addResource(exportDirectory)
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(indexer)
        .addServer(broker)
        .addServer(new EmbeddedHistorical());
  }

  @BeforeAll
  public void initTestClient()
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
  }

  @Test
  public void testMsqIngestionAndQuerying()
  {
    final String sql = StringUtils.format(
        MoreResources.MSQ.INSERT_TINY_WIKI_JSON,
        dataSource,
        Resources.DataFile.tinyWiki1Json().getAbsolutePath()
    );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord.latchableEmitter());
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    cluster.callApi().verifySqlQuery(
        "SELECT __time, isRobot, added, delta, deleted, namespace FROM %s",
        dataSource,
        "2013-08-31T01:02:33.000Z,,57,-143,200,article\n"
        + "2013-08-31T03:32:45.000Z,,459,330,129,wikipedia\n"
        + "2013-08-31T07:11:21.000Z,,123,111,12,article"
    );
  }

  @Test
  public void testExport()
  {
    final String exportSql =
        StringUtils.format(
            "INSERT INTO extern(local(exportPath => '%s'))\n"
            + "AS CSV\n"
            + "SELECT page, added, delta\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"%s\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n",
            new File(exportDirectory.get(), dataSource).getAbsolutePath(),
            Resources.DataFile.tinyWiki1Json().getAbsolutePath()
        );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(exportSql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord.latchableEmitter());

    final String selectSql = StringUtils.format(
        "SELECT page, delta, added\n"
        + "  FROM TABLE(\n"
        + "    EXTERN(\n"
        + "      '{\"type\":\"local\",\"baseDir\":\"%s\",\"filter\":\"*.csv\"}',\n"
        + "      '{\"type\":\"csv\",\"findColumnsFromHeader\":true}'\n"
        + "    )\n"
        + "  ) EXTEND (\"added\" BIGINT, \"delta\" BIGINT, \"page\" VARCHAR)\n"
        + "   WHERE delta != 0\n"
        + "   ORDER BY page",
        exportDirectory.get()
    );

    final MSQTaskReportPayload statusReport = msqApis.runTaskSqlAndGetReport(selectSql);
    Assertions.assertNotNull(statusReport);
    Assertions.assertNotNull(statusReport.getResults());

    MSQResultsReport resultsReport = statusReport.getResults();

    List<List<Object>> actualResults = new ArrayList<>();
    for (final Object[] row : resultsReport.getResults()) {
      actualResults.add(Arrays.asList(row));
    }

    List<List<Object>> expectedResults = List.of(
        List.of("Cherno Alpha", 111, 123),
        List.of("Gypsy Danger", -143, 57),
        List.of("Striker Eureka", 330, 459)
    );

    Assertions.assertEquals(
        expectedResults,
        actualResults
    );
  }

  @Test
  public void testClusterByVirtualColumn()
  {
    final String sqlTemplate =
        """
            SET rowsPerSegment = 2;
            SET groupByEnableMultiValueUnnesting = FALSE;
            REPLACE INTO %s OVERWRITE ALL
            WITH "ext" AS (
              SELECT *
              FROM TABLE(EXTERN('{"type":"local","files":["%s"]}', '{"type":"json"}'))
              EXTEND(
                "timestamp" VARCHAR,
                "added" BIGINT,
                "delta" BIGINT,
                "deleted" BIGINT,
                "page" VARCHAR,
                "city" VARCHAR,
                "country" VARCHAR,
                "user" VARCHAR
              )
            )
            SELECT
              TIME_PARSE("timestamp") AS __time,
              added,
              delta,
              deleted,
              page,
              city,
              country,
              user
            FROM "ext"
            PARTITIONED BY DAY
            CLUSTERED BY CONCAT(country, ':', city)
            """;
    final String sql = StringUtils.format(
        sqlTemplate,
        dataSource,
        Resources.DataFile.tinyWiki1Json().getAbsolutePath()
    );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord.latchableEmitter());
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    assertClusterByVirtualColumnSegments();
    assertClusterByVirtualColumnQueries();
  }

  @Test
  public void testClusterByVirtualColumnRollup()
  {
    final String sqlTemplate =
        """
            SET rowsPerSegment = 2;
            SET groupByEnableMultiValueUnnesting = FALSE;
            REPLACE INTO %s OVERWRITE ALL
            WITH "ext" AS (
              SELECT *
              FROM TABLE(EXTERN('{"type":"local","files":["%s"]}', '{"type":"json"}'))
              EXTEND(
                "timestamp" VARCHAR,
                "added" BIGINT,
                "delta" BIGINT,
                "deleted" BIGINT,
                "page" VARCHAR,
                "city" VARCHAR,
                "country" VARCHAR,
                "user" VARCHAR
              )
            )
            SELECT
              TIME_PARSE("timestamp") AS __time,
              page,
              city,
              country,
              user,
              SUM(added) as added,
              SUM(delta) as delta,
              SUM(deleted) as deleted
            FROM "ext"
            GROUP BY TIME_PARSE("timestamp"), page, city, country, user, CONCAT(country, ':', city)
            PARTITIONED BY DAY
            CLUSTERED BY CONCAT(country, ':', city)
            """;
    final String sql = StringUtils.format(
        sqlTemplate,
        dataSource,
        Resources.DataFile.tinyWiki1Json().getAbsolutePath()
    );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord.latchableEmitter());
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    assertClusterByVirtualColumnSegments();
    assertClusterByVirtualColumnQueries();
  }

  @Test
  public void testClusterByNestedVirtualColumn() throws IOException
  {
    final Path tempFile = createNestedJsonDataFile();
    final String sqlTemplate =
        """
            SET rowsPerSegment = 4;
            SET groupByEnableMultiValueUnnesting = FALSE;
            REPLACE INTO %s OVERWRITE ALL
            WITH "ext" AS (
              SELECT *
              FROM TABLE(EXTERN('{"type":"local","files":["%s"]}', '{"type":"json"}'))
              EXTEND(
                "timestamp" VARCHAR,
                "str" VARCHAR,
                "obj" TYPE('COMPLEX<json>')
              )
            )
            SELECT
              TIME_PARSE("timestamp") AS __time,
              str,
              obj
            FROM "ext"
            PARTITIONED BY DAY
            CLUSTERED BY LOWER(JSON_VALUE(obj, '$.a' RETURNING VARCHAR))
            """;
    final String sql = StringUtils.format(
        sqlTemplate,
        dataSource,
        tempFile.toAbsolutePath()
    );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord.latchableEmitter());
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    assertClusterByNestedVirtualColumnSegments();
    assertClusterByNestedVirtualColumnQueries();
  }

  @Test
  public void testClusterByNestedVirtualColumnRollup() throws IOException
  {
    final Path tempFile = createNestedJsonDataFile();
    final String sqlTemplate =
        """
            SET rowsPerSegment = 4;
            SET groupByEnableMultiValueUnnesting = FALSE;
            REPLACE INTO %s OVERWRITE ALL
            WITH "ext" AS (
              SELECT *
              FROM TABLE(EXTERN('{"type":"local","files":["%s"]}', '{"type":"json"}'))
              EXTEND(
                "timestamp" VARCHAR,
                "str" VARCHAR,
                "obj" TYPE('COMPLEX<json>')
              )
            )
            SELECT
              TIME_PARSE("timestamp") AS __time,
              str,
              obj,
              COUNT(*) AS cnt
            FROM "ext"
            GROUP BY TIME_PARSE("timestamp"), str, obj, LOWER(JSON_VALUE(obj, '$.a' RETURNING VARCHAR))
            PARTITIONED BY DAY
            CLUSTERED BY LOWER(JSON_VALUE(obj, '$.a' RETURNING VARCHAR))
            """;
    final String sql = StringUtils.format(
        sqlTemplate,
        dataSource,
        tempFile.toAbsolutePath()
    );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord.latchableEmitter());
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    assertClusterByNestedVirtualColumnSegments();
    assertClusterByNestedVirtualColumnQueries();
  }

  private void assertClusterByVirtualColumnSegments()
  {
    List<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord).stream().toList();
    Assertions.assertEquals(2, segments.size());
    VirtualColumns virtualColumns = VirtualColumns.create(
        new ExpressionVirtualColumn("v1", "concat(\"country\",':',\"city\")", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    Assertions.assertEquals(
        new DimensionRangeShardSpec(
            List.of("v1"),
            virtualColumns,
            null,
            StringTuple.create("Russia:Moscow"),
            0,
            2
        ),
        segments.get(0).getShardSpec()
    );
    Assertions.assertEquals(
        new DimensionRangeShardSpec(
            List.of("v1"),
            virtualColumns,
            StringTuple.create("Russia:Moscow"),
            null,
            1,
            2
        ),
        segments.get(1).getShardSpec()
    );
  }

  private void assertClusterByVirtualColumnQueries()
  {
    String queryId = UUID.randomUUID().toString();
    cluster.callApi().verifySqlQuery(
        "SET engine = 'msq-dart'; SET sqlQueryId = '" + queryId + "'; SELECT __time, country, city, page FROM %s ORDER BY __time",
        dataSource,
        """
            2013-08-31T01:02:33.000Z,United States,San Francisco,Gypsy Danger
            2013-08-31T03:32:45.000Z,Australia,Syndey,Striker Eureka
            2013-08-31T07:11:21.000Z,Russia,Moscow,Cherno Alpha"""
    );
    Assertions.assertEquals(2, getSegmentsScannedForDartQuery(queryId));

    queryId = UUID.randomUUID().toString();
    cluster.callApi().verifySqlQuery(
        "SET engine = 'msq-dart'; SET sqlQueryId = '" + queryId + "'; SELECT __time, country, city, page FROM %s WHERE CONCAT(country, ':', city) <= 'Russia' ORDER BY __time",
        dataSource,
        """
            2013-08-31T03:32:45.000Z,Australia,Syndey,Striker Eureka"""
    );
    Assertions.assertEquals(1, getSegmentsScannedForDartQuery(queryId));

    queryId = UUID.randomUUID().toString();
    cluster.callApi().verifySqlQuery(
        "SET engine = 'msq-dart'; SET sqlQueryId = '" + queryId + "'; SELECT __time, country, city, page FROM %s WHERE CONCAT(country, ':', city) <= 'Russia:St. Petersburg' ORDER BY __time",
        dataSource,
        """
            2013-08-31T03:32:45.000Z,Australia,Syndey,Striker Eureka
            2013-08-31T07:11:21.000Z,Russia,Moscow,Cherno Alpha"""
    );
    Assertions.assertEquals(2, getSegmentsScannedForDartQuery(queryId));
  }


  private Path createNestedJsonDataFile() throws IOException
  {
    final Path tempFile = Files.createTempFile("nested-data", ".json");
    tempFile.toFile().deleteOnExit();
    Files.writeString(tempFile,
                      """
                          {"timestamp": "2023-01-01T00:00:00", "str":"a",    "obj":{"a": "A"}}
                          {"timestamp": "2023-01-01T00:00:01", "str":"b",    "obj":{"a": "A"}}
                          {"timestamp": "2023-01-01T00:00:02", "str":"c",    "obj":{"a": "B"}}
                          {"timestamp": "2023-01-01T00:00:03", "str":"d",    "obj":{"a": "A"}}
                          {"timestamp": "2023-01-01T00:00:04", "str":"e",    "obj":{"a": "B"}}
                          {"timestamp": "2023-01-01T00:00:05", "str":"f",    "obj":{"a": "A"}}
                          {"timestamp": "2023-01-01T00:00:06", "str":"g",    "obj":{"a": "A"}}
                          """
    );
    return tempFile;
  }

  private void assertClusterByNestedVirtualColumnSegments()
  {
    // all rows in same time chunk, max rows is 4, so we expect 2 segments with a range split on 'a' since there are
    // 5 rows with 'A' and 2 rows with 'B'
    List<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord).stream().toList();
    Assertions.assertEquals(2, segments.size());

    final DimensionRangeShardSpec shardSpec0 = (DimensionRangeShardSpec) segments.get(0).getShardSpec();
    Assertions.assertEquals(1, shardSpec0.getDimensions().size());
    Assertions.assertFalse(shardSpec0.getVirtualColumns().isEmpty());
    Assertions.assertEquals(2, shardSpec0.getVirtualColumns().getVirtualColumns().length);
    Assertions.assertEquals(0, shardSpec0.getPartitionNum());

    Assertions.assertNull(shardSpec0.getStartTuple());
    Assertions.assertEquals(StringTuple.create("a"), shardSpec0.getEndTuple());

    final DimensionRangeShardSpec shardSpec1 = (DimensionRangeShardSpec) segments.get(1).getShardSpec();
    Assertions.assertEquals(shardSpec0.getDimensions(), shardSpec1.getDimensions());
    Assertions.assertEquals(shardSpec0.getVirtualColumns(), shardSpec1.getVirtualColumns());
    Assertions.assertEquals(1, shardSpec1.getPartitionNum());

    Assertions.assertEquals(StringTuple.create("a"), shardSpec1.getStartTuple());
    Assertions.assertNull(shardSpec1.getEndTuple());
  }

  private void assertClusterByNestedVirtualColumnQueries()
  {
    String queryId = UUID.randomUUID().toString();
    cluster.callApi().verifySqlQuery(
        "SET engine = 'msq-dart'; SET sqlQueryId = '" + queryId + "'; SELECT str FROM %s ORDER BY __time",
        dataSource,
        """
            a
            b
            c
            d
            e
            f
            g"""
    );
    Assertions.assertEquals(2, getSegmentsScannedForDartQuery(queryId));

    queryId = UUID.randomUUID().toString();
    cluster.callApi().verifySqlQuery(
        "SET engine = 'msq-dart'; SET sqlQueryId = '" + queryId + "'; SELECT str FROM %s WHERE LOWER(JSON_VALUE(obj, '$.a' RETURNING VARCHAR)) = 'b' ORDER BY __time",
        dataSource,
        """
            c
            e"""
    );
    Assertions.assertEquals(1, getSegmentsScannedForDartQuery(queryId));

    queryId = UUID.randomUUID().toString();
    cluster.callApi().verifySqlQuery(
        "SET engine = 'msq-dart'; SET sqlQueryId = '" + queryId + "'; SELECT str FROM %s WHERE LOWER(JSON_VALUE(obj, '$.a' RETURNING VARCHAR)) <= 'b' ORDER BY __time",
        dataSource,
        """
            a
            b
            c
            d
            e
            f
            g"""
    );
    Assertions.assertEquals(2, getSegmentsScannedForDartQuery(queryId));
  }

  private long getSegmentsScannedForDartQuery(String sqlQueryId)
  {
    ChannelCounters.Snapshot segmentChannelCounters = getDartSegmentChannelCounters(sqlQueryId);
    return segmentChannelCounters.getFiles()[0];
  }

  private ChannelCounters.Snapshot getDartSegmentChannelCounters(String sqlQueryId)
  {
    final GetQueryReportResponse reportResponse = msqApis.getDartQueryReport(sqlQueryId, broker);

    Assertions.assertNotNull(reportResponse, "Report response should not be null");
    ChannelCounters.Snapshot segmentChannelCounters =
        (ChannelCounters.Snapshot) reportResponse.getReportMap()
                                                 .findReport("multiStageQuery")
                                                 .map(r ->
                                                          ((MSQTaskReportPayload) r.getPayload()).getCounters()
                                                                                                 .snapshotForStage(0)
                                                                                                 .get(0)
                                                                                                 .getMap()
                                                                                                 .get("input0")
                                                 ).orElse(null);

    Assertions.assertNotNull(segmentChannelCounters);
    return segmentChannelCounters;
  }
}
