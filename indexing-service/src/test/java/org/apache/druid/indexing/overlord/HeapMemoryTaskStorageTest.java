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

package org.apache.druid.indexing.overlord;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.TaskLookup;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class HeapMemoryTaskStorageTest
{
  private HeapMemoryTaskStorage storage;

  @Before
  public void setUp()
  {
    storage = new HeapMemoryTaskStorage(new TaskStorageConfig(Period.days(1)));
  }

  @Test
  public void testRemoveTasksOlderThan()
  {
    final NoopTask task1 = NoopTask.create();
    final NoopTask task2 = NoopTask.create();
    storage.insert(task1, TaskStatus.success(task1.getId()));
    storage.insert(task2, TaskStatus.running(task2.getId()));

    storage.removeTasksOlderThan(DateTimes.of("2000").getMillis());
    Assert.assertNotNull(storage.getTaskInfo(task1.getId()));
    Assert.assertNotNull(storage.getTaskInfo(task2.getId()));

    storage.removeTasksOlderThan(DateTimes.of("3000").getMillis());
    Assert.assertNull(storage.getTaskInfo(task1.getId()));
    Assert.assertNotNull(storage.getTaskInfo(task2.getId()));
  }

  @Test
  public void testGetTaskInfos()
  {
    final NoopTask task1 = NoopTask.create();
    final NoopTask task2 = NoopTask.create();
    storage.insert(task1, TaskStatus.success(task1.getId()));
    storage.insert(task2, TaskStatus.running(task2.getId()));

    // Active statuses
    final List<TaskInfo<Task, TaskStatus>> taskInfosActive = storage.getTaskInfos(
        ImmutableMap.of(
            TaskLookup.TaskLookupType.ACTIVE,
            TaskLookup.ActiveTaskLookup.getInstance(),
            TaskLookup.TaskLookupType.COMPLETE,
            new TaskLookup.CompleteTaskLookup(0, DateTimes.of("1970"))
        ),
        null
    );

    Assert.assertEquals(1, taskInfosActive.size());
    Assert.assertEquals(task2.getId(), taskInfosActive.get(0).getTask().getId());

    // Complete statuses
    final List<TaskInfo<Task, TaskStatus>> taskInfosComplete = storage.getTaskInfos(
        ImmutableMap.of(
            TaskLookup.TaskLookupType.COMPLETE,
            new TaskLookup.CompleteTaskLookup(null, DateTimes.of("1970"))
        ),
        null
    );

    Assert.assertEquals(1, taskInfosComplete.size());
    Assert.assertEquals(task1.getId(), taskInfosComplete.get(0).getTask().getId());
  }
}
