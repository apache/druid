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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.server.coordination.BroadcastDatasourceLoadingSpec;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MSQControllerTaskTest
{
  private static final List<Interval> INTERVALS = Collections.singletonList(
      Intervals.of("2011-04-01/2011-04-03")
  );

  private static MSQSpec.Builder msqSpecBuilder()
  {
    return MSQSpec
        .builder()
        .destination(
            new DataSourceMSQDestination("target", Granularities.DAY, null, INTERVALS, null, null)
        )
        .query(
            new Druids.ScanQueryBuilder()
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .intervals(new MultipleIntervalSegmentSpec(INTERVALS))
                .dataSource("target")
                .build()
        )
        .columnMappings(new ColumnMappings(ImmutableList.of(new ColumnMapping("a0", "cnt"))))
        .tuningConfig(MSQTuningConfig.defaultConfig());
  }

  @Test
  public void testGetInputSourceResources()
  {
    Assert.assertTrue(createControllerTask(msqSpecBuilder()).getInputSourceResources().isEmpty());
  }

  @Test
  public void testGetDefaultLookupLoadingSpec()
  {
    MSQControllerTask controllerTask = createControllerTask(msqSpecBuilder());
    Assert.assertEquals(LookupLoadingSpec.NONE, controllerTask.getLookupLoadingSpec());
  }

  @Test
  public void testGetDefaultBroadcastDatasourceLoadingSpec()
  {
    MSQControllerTask controllerTask = createControllerTask(msqSpecBuilder());
    Assert.assertEquals(BroadcastDatasourceLoadingSpec.NONE, controllerTask.getBroadcastDatasourceLoadingSpec());
  }

  @Test
  public void testGetLookupLoadingSpecUsingLookupLoadingInfoInContext()
  {
    MSQSpec.Builder builder = MSQSpec
        .builder()
        .query(new Druids.ScanQueryBuilder()
                   .intervals(new MultipleIntervalSegmentSpec(INTERVALS))
                   .dataSource("target")
                   .context(
                       ImmutableMap.of(
                           LookupLoadingSpec.CTX_LOOKUPS_TO_LOAD, Arrays.asList("lookupName1", "lookupName2"),
                           LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, LookupLoadingSpec.Mode.ONLY_REQUIRED)
                   )
                   .build()
        )
        .columnMappings(new ColumnMappings(Collections.emptyList()))
        .tuningConfig(MSQTuningConfig.defaultConfig());

    // Validate that MSQ Controller task doesn't load any lookups even if context has lookup info populated.
    Assert.assertEquals(LookupLoadingSpec.NONE, createControllerTask(builder).getLookupLoadingSpec());
  }

  @Test
  public void testGetTaskAllocatorId()
  {
    MSQControllerTask controllerTask = createControllerTask(msqSpecBuilder());
    Assert.assertEquals(controllerTask.getId(), controllerTask.getTaskAllocatorId());
  }

  @Test
  public void testGetTaskLockType()
  {
    final DataSourceMSQDestination appendDestination
        = new DataSourceMSQDestination("target", Granularities.DAY, null, null, null, null);
    Assert.assertEquals(
        TaskLockType.SHARED,
        createControllerTask(msqSpecBuilder().destination(appendDestination)).getTaskLockType()
    );

    final DataSourceMSQDestination replaceDestination
        = new DataSourceMSQDestination("target", Granularities.DAY, null, INTERVALS, null, null);
    Assert.assertEquals(
        TaskLockType.EXCLUSIVE,
        createControllerTask(msqSpecBuilder().destination(replaceDestination)).getTaskLockType()
    );

    // With 'useConcurrentLocks' in task context
    final Map<String, Object> taskContext = Collections.singletonMap(Tasks.USE_CONCURRENT_LOCKS, true);
    final MSQControllerTask appendTaskWithContext = new MSQControllerTask(
        null,
        msqSpecBuilder().destination(appendDestination).build(),
        null,
        null,
        null,
        null,
        null,
        taskContext
    );
    Assert.assertEquals(TaskLockType.APPEND, appendTaskWithContext.getTaskLockType());

    final MSQControllerTask replaceTaskWithContext = new MSQControllerTask(
        null,
        msqSpecBuilder().destination(replaceDestination).build(),
        null,
        null,
        null,
        null,
        null,
        taskContext
    );
    Assert.assertEquals(TaskLockType.REPLACE, replaceTaskWithContext.getTaskLockType());

    // With 'useConcurrentLocks' in query context
    final Map<String, Object> queryContext = Collections.singletonMap(Tasks.USE_CONCURRENT_LOCKS, true);
    final ScanQuery query = new Druids.ScanQueryBuilder()
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .intervals(new MultipleIntervalSegmentSpec(INTERVALS))
        .dataSource("target")
        .context(queryContext)
        .build();
    Assert.assertEquals(
        TaskLockType.APPEND,
        createControllerTask(msqSpecBuilder().query(query).destination(appendDestination)).getTaskLockType()
    );
    Assert.assertEquals(
        TaskLockType.REPLACE,
        createControllerTask(msqSpecBuilder().query(query).destination(replaceDestination)).getTaskLockType()
    );
  }

  @Test
  public void testIsReady() throws Exception
  {
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
    Assert.assertTrue(createControllerTask(msqSpecBuilder()).isReady(taskActionClient));
  }

  @Test
  public void testIsReadyWithRevokedLock()
  {
    MSQControllerTask controllerTask = createControllerTask(msqSpecBuilder());
    TaskActionClient taskActionClient = new TestTaskActionClient(
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
        () -> controllerTask.isReady(taskActionClient)
    );
    Assert.assertEquals(
        "Lock of type[REPLACE] for interval[2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z] was revoked",
        exception.getMessage()
    );
  }

  private static MSQControllerTask createControllerTask(MSQSpec.Builder specBuilder)
  {
    return new MSQControllerTask("controller_1", specBuilder.build(), null, null, null, null, null, null, null);
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
