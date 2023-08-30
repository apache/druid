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

package org.apache.druid.indexing.overlord.duty;

import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.tasklogs.TaskLogKiller;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

public class TaskLogAutoCleanerTest
{
  @Test
  public void testRun() throws Exception
  {
    // Setup config and mocks
    final long durationToRetain = TimeUnit.HOURS.toMillis(2);
    TaskLogAutoCleanerConfig cleanerConfig =
        new TaskLogAutoCleanerConfig(true, 2000L, 1000L, durationToRetain);

    TaskStorage taskStorage = Mockito.mock(TaskStorage.class);
    TaskLogKiller taskLogKiller = Mockito.mock(TaskLogKiller.class);

    TaskLogAutoCleaner taskLogAutoCleaner =
        new TaskLogAutoCleaner(taskLogKiller, cleanerConfig, taskStorage);

    long expectedExpiryTime = System.currentTimeMillis() - durationToRetain;
    taskLogAutoCleaner.run();

    // Verify that kill on TaskStorage and TaskLogKiller is invoked with the correct timestamp
    Mockito.verify(taskStorage).removeTasksOlderThan(
        ArgumentMatchers.longThat(observedExpiryTime -> observedExpiryTime >= expectedExpiryTime)
    );
    Mockito.verify(taskLogKiller).killOlderThan(
        ArgumentMatchers.longThat(observedExpiryTime -> observedExpiryTime >= expectedExpiryTime)
    );
  }

  @Test
  public void testGetSchedule()
  {
    TaskLogAutoCleanerConfig cleanerConfig = new TaskLogAutoCleanerConfig(true, 2000L, 1000L, 60_000L);
    TaskLogAutoCleaner taskLogAutoCleaner = new TaskLogAutoCleaner(null, cleanerConfig, null);
    Assert.assertTrue(taskLogAutoCleaner.isEnabled());

    final DutySchedule schedule = taskLogAutoCleaner.getSchedule();
    Assert.assertEquals(cleanerConfig.getDelay(), schedule.getPeriodMillis());
    Assert.assertEquals(cleanerConfig.getInitialDelay(), schedule.getInitialDelayMillis());
  }

  @Test
  public void testIsEnabled()
  {
    TaskLogAutoCleanerConfig cleanerConfig = new TaskLogAutoCleanerConfig(false, 2000L, 1000L, 60_000L);
    TaskLogAutoCleaner taskLogAutoCleaner = new TaskLogAutoCleaner(null, cleanerConfig, null);
    Assert.assertFalse(taskLogAutoCleaner.isEnabled());
  }

}
