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

import org.junit.Assert;
import org.junit.Test;

public class AutoCompactionSnapshotTest
{
  @Test
  public void testAutoCompactionSnapshotBuilder()
  {
    final String expectedDataSource = "data";
    final AutoCompactionSnapshot.AutoCompactionScheduleStatus expectedStatus = AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING;
    AutoCompactionSnapshot.Builder builder = new AutoCompactionSnapshot.Builder(expectedDataSource, expectedStatus);

    // Increment every stats twice
    for (int i = 0; i < 2; i++) {
      builder.incrementIntervalCountSkipped(13);
      builder.incrementBytesSkipped(13);
      builder.incrementSegmentCountSkipped(13);

      builder.incrementIntervalCountCompacted(13);
      builder.incrementBytesCompacted(13);
      builder.incrementSegmentCountCompacted(13);

      builder.incrementIntervalCountAwaitingCompaction(13);
      builder.incrementBytesAwaitingCompaction(13);
      builder.incrementSegmentCountAwaitingCompaction(13);
    }

    AutoCompactionSnapshot actual = builder.build();

    Assert.assertNotNull(actual);
    Assert.assertEquals(26, actual.getSegmentCountSkipped());
    Assert.assertEquals(26, actual.getIntervalCountSkipped());
    Assert.assertEquals(26, actual.getBytesSkipped());
    Assert.assertEquals(26, actual.getBytesCompacted());
    Assert.assertEquals(26, actual.getIntervalCountCompacted());
    Assert.assertEquals(26, actual.getSegmentCountCompacted());
    Assert.assertEquals(26, actual.getBytesAwaitingCompaction());
    Assert.assertEquals(26, actual.getIntervalCountAwaitingCompaction());
    Assert.assertEquals(26, actual.getSegmentCountAwaitingCompaction());
    Assert.assertEquals(AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING, actual.getScheduleStatus());
    Assert.assertEquals(expectedDataSource, actual.getDataSource());

    AutoCompactionSnapshot expected = new AutoCompactionSnapshot(
        expectedDataSource,
        AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
        26,
        26,
        26,
        26,
        26,
        26,
        26,
        26,
        26
    );
    Assert.assertEquals(expected, actual);
  }
}
