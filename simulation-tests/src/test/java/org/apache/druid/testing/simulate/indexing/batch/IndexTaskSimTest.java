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

package org.apache.druid.testing.simulate.indexing.batch;

import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.simulate.embedded.EmbeddedBroker;
import org.apache.druid.testing.simulate.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.simulate.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.simulate.embedded.EmbeddedHistorical;
import org.apache.druid.testing.simulate.embedded.EmbeddedIndexer;
import org.apache.druid.testing.simulate.embedded.EmbeddedOverlord;
import org.apache.druid.testing.simulate.junit5.DruidSimulationTestBase;
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
public class IndexTaskSimTest extends DruidSimulationTestBase
{
  private final EmbeddedOverlord overlord = EmbeddedOverlord.create();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.create()
                               .addServer(new EmbeddedCoordinator())
                               .addServer(EmbeddedIndexer.withProps(Map.of("druid.worker.capacity", "25")))
                               .addServer(overlord)
                               .addServer(new EmbeddedHistorical())
                               .addServer(new EmbeddedBroker());
  }

  @Test
  @Timeout(60)
  public void test_run10Tasks_concurrently()
  {
    runTasksConcurrently(10);
  }

  @Test
  @Timeout(60)
  public void test_run50Tasks_oneByOne()
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

  @Test
  @Timeout(60)
  public void test_runIndexTask_forInlineDatasource()
  {
    final String txnData10Days
        = "time,item,value"
          + "\n2025-06-01,shirt,105"
          + "\n2025-06-02,trousers,210"
          + "\n2025-06-03,jeans,150"
          + "\n2025-06-04,t-shirt,53"
          + "\n2025-06-05,microwave,1099"
          + "\n2025-06-06,spoon,11"
          + "\n2025-06-07,television,1100"
          + "\n2025-06-08,plant pots,75"
          + "\n2025-06-09,shirt,99"
          + "\n2025-06-10,toys,101";

    final Task task = createIndexTaskForInlineData(TestDataSource.WIKI, txnData10Days);
    final String taskId = task.getId();

    getResult(overlord.client().runTask(taskId, task));
    verifyTaskHasSucceeded(taskId);

    // Verify that the task created 10 DAY-granularity segments
    final List<DataSegment> segments = new ArrayList<>(
        overlord.segmentsMetadataStorage().retrieveAllUsedSegments(TestDataSource.WIKI, null)
    );
    segments.sort(
        (o1, o2) -> Comparators.intervalsByStartThenEnd()
                               .compare(o1.getInterval(), o2.getInterval())
    );

    Assertions.assertEquals(10, segments.size());
    DateTime start = DateTimes.of("2025-06-01");
    for (DataSegment segment : segments) {
      Assertions.assertEquals(TestDataSource.WIKI, segment.getDataSource());
      Assertions.assertEquals(new Interval(start, Period.days(1)), segment.getInterval());
      start = start.plusDays(1);
    }

    final Object result = getResult(
        cluster.anyBroker().submitSqlQuery(
            new ClientSqlQuery("SELECT * FROM sys.segments", null, true, true, true, Map.of(), List.of())
        )
    );
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

  private void runTasksConcurrently(int count)
  {
    final DateTime jan1 = DateTimes.of("2025-01-01");
    final List<Task> tasks = IntStream.range(0, count).mapToObj(
        i -> createIndexTaskForInlineData(
            TestDataSource.KOALA,
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
      verifyTaskHasSucceeded(task.getId());
    }
  }

  private void verifyTaskHasSucceeded(String taskId)
  {
    overlord.waitUntilTaskFinishes(taskId);
    final TaskStatusResponse currentStatus = getResult(
        overlord.client().taskStatus(taskId)
    );
    Assertions.assertNotNull(currentStatus.getStatus());
    Assertions.assertEquals(
        TaskState.SUCCESS,
        currentStatus.getStatus().getStatusCode(),
        StringUtils.format("Task[%s] has failed", taskId)
    );
  }
}
