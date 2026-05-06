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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.ImmutableList;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientMSQContext;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionStatistics;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.server.compaction.Eligibility;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@code minorCompactionTaskPercent} task-context key applied by
 * {@link CompactSegments#createCompactionTask}.
 */
public class CompactSegmentsMinorTaskScalingTest
{
  private static final String DATA_SOURCE = "wiki";

  @Test
  public void testMinor_msq_defaultDoesNotScale()
  {
    final ClientCompactionTaskQuery task = buildTask(
        Eligibility.minor("uncompacted ratio below threshold"),
        CompactionEngine.MSQ,
        contextWithMaxNumTasks(10)
    );
    Assert.assertEquals(10, getMaxNumTasks(task));
  }

  @Test
  public void testMinor_msq_explicitPercentInTaskContext()
  {
    final Map<String, Object> ctx = contextWithMaxNumTasks(10);
    ctx.put(ClientMSQContext.CTX_MINOR_COMPACTION_TASK_PERCENT, 25);
    final ClientCompactionTaskQuery task = buildTask(
        Eligibility.minor("uncompacted ratio below threshold"),
        CompactionEngine.MSQ,
        ctx
    );
    Assert.assertEquals(3, getMaxNumTasks(task));
  }

  @Test
  public void testMinor_msq_floorsAtTwo()
  {
    final Map<String, Object> ctx = contextWithMaxNumTasks(3);
    ctx.put(ClientMSQContext.CTX_MINOR_COMPACTION_TASK_PERCENT, 40);
    final ClientCompactionTaskQuery task = buildTask(
        Eligibility.minor("uncompacted ratio below threshold"),
        CompactionEngine.MSQ,
        ctx
    );
    Assert.assertEquals(2, getMaxNumTasks(task));
  }

  @Test
  public void testMinor_msq_percentOneHundredIsNoOp()
  {
    final Map<String, Object> ctx = contextWithMaxNumTasks(5);
    ctx.put(ClientMSQContext.CTX_MINOR_COMPACTION_TASK_PERCENT, 100);
    final ClientCompactionTaskQuery task = buildTask(
        Eligibility.minor("uncompacted ratio below threshold"),
        CompactionEngine.MSQ,
        ctx
    );
    Assert.assertEquals(5, getMaxNumTasks(task));
  }

  @Test
  public void testMinor_msq_outOfRangePercentThrowsDefensive()
  {
    // Bad values are normally rejected at config-acceptance time by
    // ClientCompactionRunnerInfo#validateMinorCompactionTaskPercentForMSQ;
    // this asserts the defensive guard inside compactSegments still fires
    // if a bad value somehow reaches task construction.
    final Map<String, Object> ctx = contextWithMaxNumTasks(10);
    ctx.put(ClientMSQContext.CTX_MINOR_COMPACTION_TASK_PERCENT, 0);
    final DruidException thrown = Assert.assertThrows(
        DruidException.class,
        () -> buildTask(
            Eligibility.minor("uncompacted ratio below threshold"),
            CompactionEngine.MSQ,
            ctx
        )
    );
    Assert.assertEquals(DruidException.Category.DEFENSIVE, thrown.getCategory());
  }

  @Test
  public void testFull_msq_doesNotScale()
  {
    final ClientCompactionTaskQuery task = buildTask(
        Eligibility.FULL,
        CompactionEngine.MSQ,
        contextWithMaxNumTasks(5)
    );
    Assert.assertEquals(5, getMaxNumTasks(task));
  }

  @Test
  public void testMinor_native_doesNotScale()
  {
    final ClientCompactionTaskQuery task = buildTask(
        Eligibility.minor("uncompacted ratio below threshold"),
        CompactionEngine.NATIVE,
        contextWithMaxNumTasks(5)
    );
    Assert.assertEquals(5, getMaxNumTasks(task));
  }

  private static ClientCompactionTaskQuery buildTask(
      Eligibility eligibility,
      CompactionEngine engine,
      Map<String, Object> taskContext
  )
  {
    final DataSegment segment = new DataSegment(
        DATA_SOURCE,
        Intervals.of("2024-01-01/2024-01-02"),
        "v1",
        null,
        ImmutableList.of(),
        ImmutableList.of(),
        new NumberedShardSpec(0, 1),
        0,
        100L
    );
    final List<DataSegment> segments = ImmutableList.of(segment);
    final CompactionStatus status = CompactionStatus.pending(
        CompactionStatistics.create(0L, 0L, 0L, 0L),
        CompactionStatistics.create(100L, 0L, 1L, 1L),
        segments,
        "for test"
    );
    final CompactionCandidate candidate = CompactionCandidate.from(segments, null, status);
    final DataSourceCompactionConfig config = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(DATA_SOURCE)
        .withTaskContext(taskContext)
        .withEngine(engine)
        .build();
    return CompactSegments.createCompactionTask(
        candidate,
        eligibility,
        config,
        engine,
        null,
        true
    );
  }

  private static Map<String, Object> contextWithMaxNumTasks(int maxNumTasks)
  {
    final Map<String, Object> context = new HashMap<>();
    context.put(ClientMSQContext.CTX_MAX_NUM_TASKS, maxNumTasks);
    return context;
  }

  private static int getMaxNumTasks(ClientCompactionTaskQuery task)
  {
    return (int) task.getContext().get(ClientMSQContext.CTX_MAX_NUM_TASKS);
  }
}
