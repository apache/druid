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

package org.apache.druid.server.coordinator;

import org.apache.druid.server.compaction.CompactionSkipReason;
import org.apache.druid.server.compaction.CompactionSkipStatistics;
import org.apache.druid.server.compaction.CompactionStatistics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AutoCompactionSnapshotTest
{
  @Test
  public void testAutoCompactionSnapshotBuilder()
  {
    final String expectedDataSource = "data";
    final String expectedMessage = "message";
    final AutoCompactionSnapshot.Builder builder = AutoCompactionSnapshot.builder(expectedDataSource);

    // Increment every stat twice
    for (int i = 0; i < 2; i++) {
      builder.incrementSkippedStats(
          CompactionSkipReason.SKIP_OFFSET,
          CompactionStatistics.create(6, null, 6, 6)
      );
      builder.incrementSkippedStats(
          CompactionSkipReason.REJECTED_BY_SEARCH_POLICY,
          CompactionStatistics.create(7, null, 7, 7)
      );
      builder.incrementWaitingStats(CompactionStatistics.create(13, null, 13, 13));
      builder.incrementCompactedStats(CompactionStatistics.create(13, null, 13, 13));
    }

    final AutoCompactionSnapshot actual = builder.withMessage(expectedMessage).build();

    Assertions.assertNotNull(actual);
    Assertions.assertEquals(26, actual.getSegmentCountSkipped());
    Assertions.assertEquals(26, actual.getIntervalCountSkipped());
    Assertions.assertEquals(26, actual.getBytesSkipped());
    Assertions.assertEquals(26, actual.getBytesCompacted());
    Assertions.assertEquals(26, actual.getIntervalCountCompacted());
    Assertions.assertEquals(26, actual.getSegmentCountCompacted());
    Assertions.assertEquals(26, actual.getBytesAwaitingCompaction());
    Assertions.assertEquals(26, actual.getIntervalCountAwaitingCompaction());
    Assertions.assertEquals(26, actual.getSegmentCountAwaitingCompaction());
    Assertions.assertEquals(
        List.of(
            CompactionSkipStatistics.of(
                CompactionSkipReason.SKIP_OFFSET,
                CompactionStatistics.create(12, null, 12, 12)
            ),
            CompactionSkipStatistics.of(
                CompactionSkipReason.REJECTED_BY_SEARCH_POLICY,
                CompactionStatistics.create(14, null, 14, 14)
            )
        ),
        actual.getSkippedStatsByReason()
    );
    Assertions.assertEquals(AutoCompactionSnapshot.ScheduleStatus.RUNNING, actual.getScheduleStatus());
    Assertions.assertEquals(expectedDataSource, actual.getDataSource());
    Assertions.assertEquals(expectedMessage, actual.getMessage());

    AutoCompactionSnapshot expected = new AutoCompactionSnapshot(
        expectedDataSource,
        AutoCompactionSnapshot.ScheduleStatus.RUNNING,
        expectedMessage,
        26,
        26,
        26,
        26,
        26,
        26,
        26,
        26,
        26,
        actual.getSkippedStatsByReason()
    );
    Assertions.assertEquals(expected, actual);
  }
}
