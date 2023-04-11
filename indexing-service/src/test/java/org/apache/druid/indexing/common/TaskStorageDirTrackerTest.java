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

package org.apache.druid.indexing.common;

import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

public class TaskStorageDirTrackerTest
{
  public static final ImmutableList<File> MULTIPLE_FILES = ImmutableList.of(
      new File("A"),
      new File("B"),
      new File("C")
  );

  private static TaskStorageDirTracker defaultInstance()
  {
    return new TaskStorageDirTracker(MULTIPLE_FILES);
  }

  @Test
  public void testGetOrSelectTaskDir()
  {
    validateRoundRobinAllocation(defaultInstance());

    validateRoundRobinAllocation(TaskStorageDirTracker.fromConfigs(
        new WorkerConfig()
        {
          @Override
          public List<String> getBaseTaskDirs()
          {
            return MULTIPLE_FILES.stream().map(File::toString).collect(Collectors.toList());
          }
        },
        null
    ));
  }

  private void validateRoundRobinAllocation(TaskStorageDirTracker dirTracker)
  {
    // Test round-robin allocation
    Assert.assertEquals("A", dirTracker.getBaseTaskDir("task0").getPath());
    Assert.assertEquals("B", dirTracker.getBaseTaskDir("task1").getPath());
    Assert.assertEquals("C", dirTracker.getBaseTaskDir("task2").getPath());
    Assert.assertEquals("A", dirTracker.getBaseTaskDir("task3").getPath());
    Assert.assertEquals("B", dirTracker.getBaseTaskDir("task4").getPath());
    Assert.assertEquals("C", dirTracker.getBaseTaskDir("task5").getPath());

    // Test that the result is always the same
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals("A", dirTracker.getBaseTaskDir("task0").getPath());
    }
  }

  @Test
  public void testFallBackToTaskConfig()
  {
    final TaskStorageDirTracker tracker = TaskStorageDirTracker.fromConfigs(
        new WorkerConfig(),
        new TaskConfigBuilder().setBaseDir("A").build()
    );

    Assert.assertEquals("A/persistent/task", tracker.getBaseTaskDir("task0").getPath());
    Assert.assertEquals("A/persistent/task", tracker.getBaseTaskDir("task1").getPath());
    Assert.assertEquals("A/persistent/task", tracker.getBaseTaskDir("task2").getPath());
    Assert.assertEquals("A/persistent/task", tracker.getBaseTaskDir("task3").getPath());
    Assert.assertEquals("A/persistent/task", tracker.getBaseTaskDir("task1").getPath());
    Assert.assertEquals("A/persistent/task", tracker.getBaseTaskDir("task10293721").getPath());
  }

  @Test
  public void testAddTask()
  {
    TaskStorageDirTracker dirTracker = defaultInstance();

    // Test add after get. task0 -> "A"
    Assert.assertEquals("A", dirTracker.getBaseTaskDir("task0").getPath());
    dirTracker.addTask("task0", new File("A"));
    Assert.assertEquals("A", dirTracker.getBaseTaskDir("task0").getPath());

    // Assign base path directly
    dirTracker.addTask("task1", new File("C"));
    Assert.assertEquals("C", dirTracker.getBaseTaskDir("task1").getPath());
  }

  @Test
  public void testAddTaskThrowsISE()
  {
    TaskStorageDirTracker dirTracker = defaultInstance();

    // Test add after get. task0 -> "A"
    Assert.assertEquals("A", dirTracker.getBaseTaskDir("task0").getPath());
    Assert.assertThrows(ISE.class, () -> dirTracker.addTask("task0", new File("B")));
  }
}
