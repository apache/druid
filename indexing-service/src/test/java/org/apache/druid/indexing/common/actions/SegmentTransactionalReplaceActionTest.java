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

package org.apache.druid.indexing.common.actions;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.SegmentUpgradeMetrics;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

public class SegmentTransactionalReplaceActionTest
{
  private static final String DATA_SOURCE = "wiki";
  private static final String SUPERVISOR_ID = "sup1";

  // Larger than SegmentTransactionalReplaceAction.NOTIFIED_LOG_SAMPLE_SIZE (10) so the bounded/truncated log path runs.
  private static final int MORE_THAN_SAMPLE_SIZE = 12;

  private final SegmentTransactionalReplaceAction action =
      SegmentTransactionalReplaceAction.create(ImmutableSet.of(), null);

  private StubServiceEmitter emitter;
  private SupervisorManager supervisorManager;
  private TaskActionToolbox toolbox;
  private Task task;

  @Before
  public void setUp()
  {
    emitter = new StubServiceEmitter("test", "localhost");
    supervisorManager = EasyMock.createMock(SupervisorManager.class);
    toolbox = EasyMock.createMock(TaskActionToolbox.class);
    task = NoopTask.forDatasource(DATA_SOURCE);

    EasyMock.expect(toolbox.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(toolbox.getSupervisorManager()).andReturn(supervisorManager).anyTimes();
  }

  @Test
  public void testNoActiveSupervisorStillEmitsPersistedPerSegment()
  {
    EasyMock.expect(supervisorManager.getActiveSupervisorIdsForDatasourceWithAppendLock(DATA_SOURCE))
            .andReturn(List.of());
    // registerUpgradedPendingSegmentOnSupervisor must not be called when there is no active supervisor.
    EasyMock.replay(toolbox, supervisorManager);

    action.registerUpgradedPendingSegmentsOnSupervisor(task, toolbox, records(2));

    Assert.assertEquals(2, emitter.getMetricEventCount(SegmentUpgradeMetrics.PERSISTED));
    EasyMock.verify(supervisorManager);
  }

  @Test
  public void testActiveSupervisorRegistersEachSegment()
  {
    EasyMock.expect(supervisorManager.getActiveSupervisorIdsForDatasourceWithAppendLock(DATA_SOURCE))
            .andReturn(List.of(SUPERVISOR_ID));
    EasyMock.expect(
        supervisorManager.registerUpgradedPendingSegmentOnSupervisor(EasyMock.eq(SUPERVISOR_ID), EasyMock.anyObject())
    ).andReturn(OptionalInt.of(2)).times(3);
    EasyMock.replay(toolbox, supervisorManager);

    action.registerUpgradedPendingSegmentsOnSupervisor(task, toolbox, records(3));

    Assert.assertEquals(3, emitter.getMetricEventCount(SegmentUpgradeMetrics.PERSISTED));
    EasyMock.verify(supervisorManager);
  }

  @Test
  public void testBatchLargerThanSampleSizeRegistersEverySegment()
  {
    EasyMock.expect(supervisorManager.getActiveSupervisorIdsForDatasourceWithAppendLock(DATA_SOURCE))
            .andReturn(List.of(SUPERVISOR_ID));
    EasyMock.expect(
        supervisorManager.registerUpgradedPendingSegmentOnSupervisor(EasyMock.eq(SUPERVISOR_ID), EasyMock.anyObject())
    ).andReturn(OptionalInt.of(1)).times(MORE_THAN_SAMPLE_SIZE);
    EasyMock.replay(toolbox, supervisorManager);

    action.registerUpgradedPendingSegmentsOnSupervisor(task, toolbox, records(MORE_THAN_SAMPLE_SIZE));

    Assert.assertEquals(MORE_THAN_SAMPLE_SIZE, emitter.getMetricEventCount(SegmentUpgradeMetrics.PERSISTED));
    EasyMock.verify(supervisorManager);
  }

  @Test
  public void testSegmentsNotRegisteredOnSupervisorAreStillPersisted()
  {
    // An empty result models a non-seekable-stream supervisor or a registration failure; the segment is still counted
    // as persisted but contributes no notified-task summary.
    EasyMock.expect(supervisorManager.getActiveSupervisorIdsForDatasourceWithAppendLock(DATA_SOURCE))
            .andReturn(List.of(SUPERVISOR_ID));
    EasyMock.expect(
        supervisorManager.registerUpgradedPendingSegmentOnSupervisor(EasyMock.eq(SUPERVISOR_ID), EasyMock.anyObject())
    ).andReturn(OptionalInt.empty()).times(2);
    EasyMock.replay(toolbox, supervisorManager);

    action.registerUpgradedPendingSegmentsOnSupervisor(task, toolbox, records(2));

    Assert.assertEquals(2, emitter.getMetricEventCount(SegmentUpgradeMetrics.PERSISTED));
    EasyMock.verify(supervisorManager);
  }

  @Test
  public void testFullPerSegmentDetailLoggedAtDebug()
  {
    final Level originalLevel = LogManager.getLogger(SegmentTransactionalReplaceAction.class).getLevel();
    Configurator.setLevel(SegmentTransactionalReplaceAction.class.getName(), Level.DEBUG);
    try {
      EasyMock.expect(supervisorManager.getActiveSupervisorIdsForDatasourceWithAppendLock(DATA_SOURCE))
              .andReturn(List.of(SUPERVISOR_ID));
      EasyMock.expect(
          supervisorManager.registerUpgradedPendingSegmentOnSupervisor(EasyMock.eq(SUPERVISOR_ID), EasyMock.anyObject())
      ).andReturn(OptionalInt.of(1)).times(MORE_THAN_SAMPLE_SIZE);
      EasyMock.replay(toolbox, supervisorManager);

      action.registerUpgradedPendingSegmentsOnSupervisor(task, toolbox, records(MORE_THAN_SAMPLE_SIZE));

      Assert.assertEquals(MORE_THAN_SAMPLE_SIZE, emitter.getMetricEventCount(SegmentUpgradeMetrics.PERSISTED));
      EasyMock.verify(supervisorManager);
    }
    finally {
      Configurator.setLevel(SegmentTransactionalReplaceAction.class.getName(), originalLevel);
    }
  }

  private static List<PendingSegmentRecord> records(int count)
  {
    final List<PendingSegmentRecord> records = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      records.add(
          PendingSegmentRecord.create(
              new SegmentIdWithShardSpec(
                  DATA_SOURCE,
                  Intervals.of("2024/2025"),
                  "v1",
                  new NumberedShardSpec(i, 0)
              ),
              "sequenceName",
              "prevId" + i,
              "upgradedFrom" + i,
              "taskAllocatorId"
          )
      );
    }
    return records;
  }
}
