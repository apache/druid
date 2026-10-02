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

import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TableProjectionSpec;
import org.apache.druid.indexer.granularity.SegmentGranularitySpec;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Embedded tests for batch {@link IndexTask} using inline datasources.
 */
public class IndexTaskTest extends EmbeddedClusterTestBase
{
  protected final EmbeddedBroker broker = new EmbeddedBroker();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer().addProperty("druid.worker.capacity", "25");
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedHistorical historical = new EmbeddedHistorical();
  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(indexer)
                               .addServer(historical)
                               .addServer(broker)
                               .addServer(new EmbeddedRouter());
  }

  @Test
  @Timeout(20)
  public void test_runIndexTask_forInlineDatasource()
  {
    final String taskId = EmbeddedClusterApis.newTaskId(dataSource);
    final IndexTask task = createIndexTaskForInlineData(
        taskId,
        Resources.InlineData.CSV_10_DAYS
    );

    cluster.callApi().runTask(task, overlord);

    // Verify that the task created 10 DAY-granularity segments
    final List<DataSegment> segments = new ArrayList<>(
        overlord.bindings().segmentsMetadataStorage().retrieveAllUsedSegments(dataSource, null)
    );
    segments.sort(
        (o1, o2) -> Comparators.intervalsByStartThenEnd()
                               .compare(o1.getInterval(), o2.getInterval())
    );

    Assertions.assertEquals(10, segments.size());
    DateTime start = DateTimes.of("2025-06-01");
    for (DataSegment segment : segments) {
      Assertions.assertEquals(dataSource, segment.getDataSource());
      Assertions.assertEquals(new Interval(start, Period.days(1)), segment.getInterval());
      start = start.plusDays(1);
    }

    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    Assertions.assertEquals(
        Resources.InlineData.CSV_10_DAYS,
        cluster.runSql("SELECT * FROM %s", dataSource)
    );
    Assertions.assertEquals("10", cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
  }

  /**
   * Batch ingestion driven by a plain {@link TableProjectionSpec} base table instead of the legacy top-level schema
   * fields. The spec lowers into the standard ingest inputs, so declared column order becomes segment sort order
   * (item before __time here — not the legacy time-first sort) and the spec's query-granularity carrier floors
   * {@code __time}, both observable in the ingested segment.
   */
  @Test
  @Timeout(60)
  public void test_runIndexTask_withBaseTableSpec()
  {
    final String taskId = EmbeddedClusterApis.newTaskId(dataSource);
    final TableProjectionSpec baseTable = TableProjectionSpec
        .builder()
        .columns(
            new StringDimensionSchema("item"),
            new LongDimensionSchema("__time"),
            new LongDimensionSchema("value")
        )
        .build()
        .withQueryGranularity(Granularities.HOUR);
    final IndexTask task = TaskBuilder
        .ofTypeIndex()
        .isoTimestampColumn("time")
        .csvInputFormatWithColumns("time", "item", "value")
        .inlineInputSourceWithData(
            "2025-06-01T05:30:00.000Z,cherry,3"
            + "\n2025-06-01T01:45:00.000Z,banana,2"
            + "\n2025-06-01T09:10:00.000Z,apple,1"
        )
        .dataSchema(
            schema -> schema
                .withBaseTable(baseTable)
                .withSegmentGranularity(new SegmentGranularitySpec(Granularities.DAY, null))
        )
        .dataSource(dataSource)
        .withId(taskId);

    cluster.callApi().runTask(task, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    // A scan without ORDER BY returns rows in segment order: sorted by item first (declared order), with __time
    // floored to the hour by the carrier, even though the rows arrived unsorted with unfloored timestamps.
    Assertions.assertEquals(
        "apple,1,2025-06-01T09:00:00.000Z"
        + "\nbanana,2,2025-06-01T01:00:00.000Z"
        + "\ncherry,3,2025-06-01T05:00:00.000Z",
        cluster.runSql("SELECT item, \"value\", __time FROM %s", dataSource)
    );
  }

  @Test
  @Timeout(20)
  public void test_run10Tasks_concurrently()
  {
    runTasksConcurrently(10);
  }

  @Test
  @Timeout(20)
  public void test_run25Tasks_oneByOne()
  {
    for (int i = 0; i < 25; ++i) {
      runTasksConcurrently(1);
    }
  }

  @Test
  @Timeout(20)
  public void test_run25Tasks_concurrently()
  {
    runTasksConcurrently(25);
  }

  @Test
  @Timeout(20)
  public void test_run100Tasks_concurrently()
  {
    runTasksConcurrently(100);
  }

  private IndexTask createIndexTaskForInlineData(String taskId, String inlineDataCsv)
  {
    return MoreResources.Task.BASIC_INDEX
        .get()
        .inlineInputSourceWithData(inlineDataCsv)
        .dataSource(dataSource)
        .withId(taskId);
  }

  /**
   * Runs the given number of concurrent batch {@link IndexTask} for {@link #dataSource}.
   * Each task ingests a single segment containing 1 row of data.
   */
  private void runTasksConcurrently(int count)
  {
    final DateTime jan1 = DateTimes.of("2025-01-01");

    final List<String> taskIds = IntStream.range(0, count).mapToObj(
        i -> EmbeddedClusterApis.newTaskId(dataSource)
    ).collect(Collectors.toList());

    int index = 0;
    for (String taskId : taskIds) {
      index++;
      final IndexTask task = createIndexTaskForInlineData(
          taskId,
          StringUtils.format(
              "%s,%s,%d",
              jan1.plusDays(index), "item " + index, index
          )
      );
      cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    }
    for (String taskId : taskIds) {
      cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    }
  }
}
