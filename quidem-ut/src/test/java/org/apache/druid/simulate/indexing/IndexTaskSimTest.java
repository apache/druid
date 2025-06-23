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

package org.apache.druid.simulate.indexing;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.simulate.EmbeddedBroker;
import org.apache.druid.testing.simulate.EmbeddedCoordinator;
import org.apache.druid.testing.simulate.EmbeddedDruidCluster;
import org.apache.druid.testing.simulate.EmbeddedHistorical;
import org.apache.druid.testing.simulate.EmbeddedIndexer;
import org.apache.druid.testing.simulate.EmbeddedOverlord;
import org.apache.druid.testing.simulate.junit5.IndexingSimulationTestBase;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Simulation tests for batch {@link IndexTask} using inline datasources.
 */
public class IndexTaskSimTest extends IndexingSimulationTestBase
{
  private static final String HEADERS = "time,item,value";
  private static final String CSV_DATA_10_DAYS =
      "2025-06-01T00:00:00.000Z,shirt,105"
      + "\n2025-06-02T00:00:00.000Z,trousers,210"
      + "\n2025-06-03T00:00:00.000Z,jeans,150"
      + "\n2025-06-04T00:00:00.000Z,t-shirt,53"
      + "\n2025-06-05T00:00:00.000Z,microwave,1099"
      + "\n2025-06-06T00:00:00.000Z,spoon,11"
      + "\n2025-06-07T00:00:00.000Z,television,1100"
      + "\n2025-06-08T00:00:00.000Z,plant pots,75"
      + "\n2025-06-09T00:00:00.000Z,shirt,99"
      + "\n2025-06-10T00:00:00.000Z,toys,101";

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    indexer.addProperty("druid.worker.capacity", "25");
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(indexer)
                               .addServer(overlord)
                               .addServer(historical)
                               .addServer(broker);
  }

  @Test
  @Timeout(60)
  public void test_runIndexTask_forInlineDatasource()
  {
    final Task task = createIndexTaskForInlineData(dataSource, HEADERS + "\n" + CSV_DATA_10_DAYS);
    final String taskId = task.getId();

    getResult(overlord.client().runTask(taskId, task));
    waitForTaskToSucceed(taskId, overlord);

    // Verify that the task created 10 DAY-granularity segments
    final List<DataSegment> segments = new ArrayList<>(
        overlord.segmentsMetadataStorage().retrieveAllUsedSegments(dataSource, null)
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

    // Wait for segments to be queryable
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );
    Assertions.assertEquals(CSV_DATA_10_DAYS, runSql("SELECT * FROM %s", dataSource));
    Assertions.assertEquals("10", runSql("SELECT COUNT(*) FROM %s", dataSource));
  }

  @Test
  @Timeout(60)
  public void test_run10Tasks_concurrently()
  {
    runTasksConcurrently(10);
  }

  @Test
  @Timeout(60)
  public void test_run25Tasks_oneByOne()
  {
    for (int i = 0; i < 25; ++i) {
      runTasksConcurrently(1);
    }
  }

  @Test
  @Timeout(60)
  public void test_run25Tasks_concurrently()
  {
    runTasksConcurrently(25);
  }

  @Test
  @Timeout(60)
  public void test_run100Tasks_concurrently()
  {
    runTasksConcurrently(100);
  }

  private static Task createIndexTaskForInlineData(String dataSource, String inlineData)
  {
    final String taskId = IdUtils.newTaskId("sim_test_index_inline", TestDataSource.WIKI, null);
    return new IndexTask(
        taskId,
        null,
        new IndexTask.IndexIngestionSpec(
            DataSchema.builder()
                      .withTimestamp(new TimestampSpec("time", null, null))
                      .withDimensions(DimensionsSpec.EMPTY)
                      .withDataSource(dataSource)
                      .build(),
            new IndexTask.IndexIOConfig(
                new InlineInputSource(inlineData),
                new CsvInputFormat(null, null, null, true, 0, false),
                true,
                false
            ),
            null
        ),
        Map.of()
    );
  }

  /**
   * Runs the given number of concurrent batch {@link IndexTask} for {@link #dataSource}.
   * Each task ingests a single segment containing 1 row of data.
   */
  private void runTasksConcurrently(int count)
  {
    final DateTime jan1 = DateTimes.of("2025-01-01");
    final List<Task> tasks = IntStream.range(0, count).mapToObj(
        i -> createIndexTaskForInlineData(
            dataSource,
            StringUtils.format(
                "time,item,value\n%s,%s,%d",
                jan1.plusDays(i), "item " + i, i
            )
        )
    ).collect(Collectors.toList());

    for (Task task : tasks) {
      getResult(
          overlord.client().runTask(task.getId(), task)
      );
    }
    for (Task task : tasks) {
      waitForTaskToSucceed(task.getId(), overlord);
    }
  }
}
