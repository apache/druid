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

package org.apache.druid.testing.simulate.task;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
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
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Simulation tests for batch {@link IndexTask} using inline datasources.
 */
public class IndexTaskSimTest
{
  private static final EmbeddedOverlord OVERLORD = EmbeddedOverlord.create();
  private static final EmbeddedDruidCluster CLUSTER
      = EmbeddedDruidCluster.builder()
                            .with(new EmbeddedCoordinator())
                            .with(EmbeddedIndexer.withProps(Map.of("druid.worker.capacity", "25")))
                            .with(OVERLORD)
                            .with(new EmbeddedHistorical())
                            .with(new EmbeddedBroker())
                            .withDb()
                            .build();

  @ClassRule
  public static final RuleChain CLUSTER_RULE_CHAIN = CLUSTER.ruleChain();

  @Test(timeout = 60_000L)
  public void test_run10Tasks_concurrently()
  {
    runTasksConcurrently(10);
  }

  @Test(timeout = 60_000L)
  public void test_run50Tasks_oneByOne()
  {
    for (int i = 0; i < 25; ++i) {
      runTasksConcurrently(1);
    }
  }

  @Test(timeout = 60_000L)
  public void test_run25Tasks_concurrently()
  {
    runTasksConcurrently(25);
  }

  @Test(timeout = 60_000L)
  public void test_run100Tasks_concurrently()
  {
    runTasksConcurrently(100);
  }

  @Test
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

    getResult(OVERLORD.client().runTask(taskId, task));
    verifyTaskHasSucceeded(taskId);

    // Verify that the task created 10 DAY-granularity segments
    final List<DataSegment> segments = new ArrayList<>(
        OVERLORD.segmentsMetadataStorage().retrieveAllUsedSegments(TestDataSource.WIKI, null)
    );
    segments.sort(
        (o1, o2) -> Comparators.intervalsByStartThenEnd()
                               .compare(o1.getInterval(), o2.getInterval())
    );

    Assert.assertEquals(10, segments.size());
    DateTime start = DateTimes.of("2025-06-01");
    for (DataSegment segment : segments) {
      Assert.assertEquals(TestDataSource.WIKI, segment.getDataSource());
      Assert.assertEquals(
          new Interval(start, Period.days(1)),
          segment.getInterval()
      );
      start = start.plusDays(1);
    }

    // TODO: wait for segments to be loaded and broker to be aware of it

    final Object result = getResult(
        CLUSTER.anyBroker().submitSqlQuery(
            new ClientSqlQuery("SELECT * FROM sys.segments", null, true, true, true, Map.of(), List.of())
        )
    );
    System.out.println("Query result = " + result);
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
          OVERLORD.client().runTask(task.getId(), task)
      );
    }
    for (Task task : tasks) {
      verifyTaskHasSucceeded(task.getId());
    }
  }

  private static void verifyTaskHasSucceeded(String taskId)
  {
    OVERLORD.waitUntilTaskFinishes(taskId);
    final TaskStatusResponse currentStatus = getResult(
        OVERLORD.client().taskStatus(taskId)
    );
    Assert.assertNotNull(currentStatus.getStatus());
    Assert.assertEquals(
        StringUtils.format("Task[%s] has failed", taskId),
        TaskState.SUCCESS,
        currentStatus.getStatus().getStatusCode()
    );
  }


  private static <T> T getResult(ListenableFuture<T> future)
  {
    return FutureUtils.getUnchecked(future, true);
  }
}
