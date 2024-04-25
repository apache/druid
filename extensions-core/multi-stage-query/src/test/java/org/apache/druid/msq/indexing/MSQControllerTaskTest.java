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

package org.apache.druid.msq.indexing;

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class MSQControllerTaskTest
{
  private final List<Interval> INTERVALS =
      Collections.singletonList(Intervals.of(
          "2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"));

  private final MSQSpec MSQ_SPEC = MSQSpec
      .builder()
      .destination(new DataSourceMSQDestination(
          "target",
          Granularities.DAY,
          null,
          INTERVALS
      ))
      .query(new Druids.ScanQueryBuilder()
                 .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                 .legacy(false)
                 .intervals(new MultipleIntervalSegmentSpec(INTERVALS))
                 .dataSource("target")
                 .build()
      )
      .columnMappings(new ColumnMappings(ImmutableList.of(new ColumnMapping("a0", "cnt"))))
      .tuningConfig(MSQTuningConfig.defaultConfig())
      .build();

  @Test
  public void testGetInputSourceResources()
  {
    MSQControllerTask controllerTask = new MSQControllerTask(
        null,
        MSQ_SPEC,
        null,
        null,
        null,
        null,
        null,
        null
    );
    Assert.assertTrue(controllerTask.getInputSourceResources().isEmpty());
  }

  @Test
  public void testGetTaskAllocatorId()
  {
    final String taskId = "taskId";
    MSQControllerTask controllerTask = new MSQControllerTask(
        taskId,
        MSQ_SPEC,
        null,
        null,
        null,
        null,
        null,
        null
    );
    Assert.assertEquals(taskId, controllerTask.getTaskAllocatorId());
  }

  @Test
  public void testIsReady() throws Exception
  {
    final String taskId = "taskId";
    MSQControllerTask controllerTask = new MSQControllerTask(
        taskId,
        MSQ_SPEC,
        null,
        null,
        null,
        null,
        null,
        null
    );
    TestTaskActionClient taskActionClient = new TestTaskActionClient(
        new TimeChunkLock(
            TaskLockType.REPLACE,
            "groupId",
            "dataSource",
            INTERVALS.get(0),
            "0",
            0
        )
    );
    Assert.assertTrue(controllerTask.isReady(taskActionClient));
  }

  @Test
  public void testIsReadyWithRevokedLock()
  {
    final String taskId = "taskId";
    MSQControllerTask controllerTask = new MSQControllerTask(
        taskId,
        MSQ_SPEC,
        null,
        null,
        null,
        null,
        null,
        null
    );
    TestTaskActionClient taskActionClient = new TestTaskActionClient(
        new TimeChunkLock(
            TaskLockType.REPLACE,
            "groupId",
            "dataSource",
            INTERVALS.get(0),
            "0",
            0,
            true
        )
    );
    DruidException exception = Assert.assertThrows(
        DruidException.class,
        () -> controllerTask.isReady(taskActionClient));
    Assert.assertEquals(
        "Lock of type[REPLACE] for interval[2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z] was revoked",
        exception.getMessage());
  }

  private static class TestTaskActionClient implements TaskActionClient
  {
    private final TaskLock taskLock;

    TestTaskActionClient(TaskLock taskLock)
    {
      this.taskLock = taskLock;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <RetType> RetType submit(TaskAction<RetType> taskAction)
    {
      if (!(taskAction instanceof TimeChunkLockTryAcquireAction)) {
        throw new ISE("action[%s] is not supported", taskAction);
      }
      return (RetType) taskLock;
    }
  }
}
