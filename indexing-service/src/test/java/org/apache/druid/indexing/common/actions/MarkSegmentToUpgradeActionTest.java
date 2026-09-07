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

import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TimeChunkLockRequest;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MarkSegmentToUpgradeActionTest
{
  private TaskActionTestKit actionTestKit;

  @BeforeEach
  public void setUp()
  {
    actionTestKit = new TaskActionTestKit();
    actionTestKit.before();
  }

  @AfterEach
  public void tearDown()
  {
    actionTestKit.after();
  }

  private static final String DATA_SOURCE = "test_dataSource";
  private static final Interval INTERVAL_2026_01 = Intervals.of("2026-01-01/2026-01-02");
  private static final Interval INTERVAL_2026_02 = Intervals.of("2026-01-02/2026-01-03");
  private static final String VERSION = "2026-01-01T00:00:00.000Z";

  private static final DataSegment SEGMENT1 =
      DataSegment.builder(SegmentId.of(DATA_SOURCE, INTERVAL_2026_01, VERSION, 0))
                 .shardSpec(new LinearShardSpec(0))
                 .build();

  private static final DataSegment SEGMENT2 =
      DataSegment.builder(SegmentId.of(DATA_SOURCE, INTERVAL_2026_01, VERSION, 1))
                 .shardSpec(new LinearShardSpec(1))
                 .build();

  private static final DataSegment SEGMENT3 =
      DataSegment.builder(SegmentId.of(DATA_SOURCE, INTERVAL_2026_02, VERSION, 0))
                 .shardSpec(new LinearShardSpec(0))
                 .build();

  @Test
  public void test_segmentsSuccessfullyInsertedIntoUpgradeTable() throws Exception
  {
    final Task task = NoopTask.forDatasource(DATA_SOURCE);
    actionTestKit.getTaskLockbox().add(task);
    actionTestKit.getTaskLockbox()
                 .lock(task, new TimeChunkLockRequest(TaskLockType.REPLACE, task, INTERVAL_2026_01, null), 5000);
    actionTestKit.getTaskLockbox()
                 .lock(task, new TimeChunkLockRequest(TaskLockType.REPLACE, task, INTERVAL_2026_02, null), 5000);

    final MarkSegmentToUpgradeAction action = new MarkSegmentToUpgradeAction(DATA_SOURCE, Set.of(SEGMENT1, SEGMENT2, SEGMENT3));

    final Integer insertedCount = action.perform(task, actionTestKit.getTaskActionToolbox());
    assertEquals(3, insertedCount.intValue());
    final int deletedCount = actionTestKit.getMetadataStorageCoordinator().deleteUpgradeSegmentsForTask(task.getId());
    assertEquals(3, deletedCount);
  }

  @Test
  public void test_failsWhenSegmentsNotCoveredByReplaceLock() throws Exception
  {
    final Task task = NoopTask.forDatasource(DATA_SOURCE);
    actionTestKit.getTaskLockbox().add(task);
    actionTestKit.getTaskLockbox()
                 .lock(task, new TimeChunkLockRequest(TaskLockType.REPLACE, task, INTERVAL_2026_01, null), 5000);

    final MarkSegmentToUpgradeAction action = new MarkSegmentToUpgradeAction(DATA_SOURCE, Set.of(SEGMENT1, SEGMENT2, SEGMENT3));

    DruidException exception = assertThrows(
        DruidException.class,
        () -> action.perform(task, actionTestKit.getTaskActionToolbox())
    );
    assertTrue(exception.getMessage().contains("Segments to upgrade must be covered by a REPLACE lock"));
  }

  @Test
  public void test_failsWithExclusiveLockInsteadOfReplaceLock() throws Exception
  {
    final Task task = NoopTask.forDatasource(DATA_SOURCE);
    actionTestKit.getTaskLockbox().add(task);
    actionTestKit.getTaskLockbox()
                 .lock(task, new TimeChunkLockRequest(TaskLockType.EXCLUSIVE, task, INTERVAL_2026_01, null), 5000);

    final MarkSegmentToUpgradeAction action = new MarkSegmentToUpgradeAction(DATA_SOURCE, Set.of(SEGMENT1, SEGMENT2));

    DruidException exception = assertThrows(
        DruidException.class,
        () -> action.perform(task, actionTestKit.getTaskActionToolbox())
    );
    assertTrue(exception.getMessage().contains("Segments to upgrade must be covered by a REPLACE lock"));
  }

  @Test
  public void test_emptySegmentsList()
  {
    final Task task = NoopTask.forDatasource(DATA_SOURCE);
    actionTestKit.getTaskLockbox().add(task);

    final MarkSegmentToUpgradeAction action = new MarkSegmentToUpgradeAction(DATA_SOURCE, Set.of());

    DruidException exception = assertThrows(
        DruidException.class,
        () -> action.perform(task, actionTestKit.getTaskActionToolbox())
    );
    assertTrue(exception.getMessage().contains("No segment to commit"));
  }

  @Test
  public void test_singleSegmentUpgrade() throws Exception
  {
    final Task task = NoopTask.forDatasource(DATA_SOURCE);
    actionTestKit.getTaskLockbox().add(task);
    actionTestKit.getTaskLockbox()
                 .lock(task, new TimeChunkLockRequest(TaskLockType.REPLACE, task, INTERVAL_2026_01, null), 5000);

    final MarkSegmentToUpgradeAction action = new MarkSegmentToUpgradeAction(DATA_SOURCE, Set.of(SEGMENT1));

    final Integer insertedCount = action.perform(task, actionTestKit.getTaskActionToolbox());
    assertEquals(1, insertedCount.intValue());
    final int deletedCount = actionTestKit.getMetadataStorageCoordinator().deleteUpgradeSegmentsForTask(task.getId());
    assertEquals(1, deletedCount);
  }
}
