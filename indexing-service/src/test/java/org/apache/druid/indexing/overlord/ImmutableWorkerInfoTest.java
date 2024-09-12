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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ImmutableWorkerInfoTest
{

  private static final String MSQ_CONTROLLER_TYPE = "query_controller";
  private static final String RANDOM_TYPE = "blah";
  private static final String AVAILABILITY_GROUP = "grp1";

  @Test
  public void testSerde() throws Exception
  {
    ImmutableWorkerInfo workerInfo = new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    );
    ObjectMapper mapper = new DefaultObjectMapper();
    final ImmutableWorkerInfo serde = mapper.readValue(
        mapper.writeValueAsString(workerInfo),
        ImmutableWorkerInfo.class
    );
    Assert.assertEquals(workerInfo, serde);
  }

  @Test
  public void testEqualsAndSerde()
  {
    // Everything equal
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), true);

    // same worker different category
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker", "192.0.0.1", 10, "v1", "c1"
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker", "192.0.0.1", 10, "v1", "c2"
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), false);

    // different worker same tasks
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker1", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker2", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), false);

    // same worker different task groups
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        2,
        ImmutableSet.of("grp3", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), false);

    // same worker different tasks
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker1", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker2", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task3"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), false);

    // same worker different capacity
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker1", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        3,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker2", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), false);

    // same worker different lastCompletedTaskTime
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker1", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        3,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker2", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:02Z")
    ), false);

    // same worker different blacklistedUntil
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker1", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        3,
        new HashMap<>(),
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:01Z"),
        DateTimes.of("2017-07-30")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker2", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
        ),
        2,
        new HashMap<>(),
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:02Z"),
        DateTimes.of("2017-07-31")
    ), false);
  }

  @Test
  public void test_canRunTask()
  {
    ImmutableWorkerInfo workerInfo = new ImmutableWorkerInfo(
        new Worker("http", "testWorker2", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY),
        6,
        new HashMap<>(),
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        DateTimes.of("2015-01-01T01:01:02Z")
    );


    // Parallel index task
    TaskResource taskResource0 = mock(TaskResource.class);
    when(taskResource0.getRequiredCapacity()).thenReturn(3);
    Task parallelIndexTask = mock(ParallelIndexSupervisorTask.class);
    when(parallelIndexTask.getType()).thenReturn(ParallelIndexSupervisorTask.TYPE);
    when(parallelIndexTask.getTaskResource()).thenReturn(taskResource0);

    Map<String, Number> taskLimitsMap = new HashMap<>();

    // Since task satisifies parallel and total slot constraints, can run
    taskLimitsMap.put(ParallelIndexSupervisorTask.TYPE, 0.5);
    Assert.assertTrue(workerInfo.canRunTask(parallelIndexTask, taskLimitsMap));

    // Since task fails the parallel slot constraint, it cannot run (3 > 1)
    taskLimitsMap.put(ParallelIndexSupervisorTask.TYPE, 0.1);
    Assert.assertFalse(workerInfo.canRunTask(parallelIndexTask, taskLimitsMap));

    taskLimitsMap.put(ParallelIndexSupervisorTask.TYPE, 1.0);
    Assert.assertTrue(workerInfo.canRunTask(parallelIndexTask, taskLimitsMap));


    // Some other indexing task
    TaskResource taskResource1 = mock(TaskResource.class);
    when(taskResource1.getRequiredCapacity()).thenReturn(5);
    Task anyOtherTask = mock(IndexTask.class);
    when(anyOtherTask.getType()).thenReturn(IndexTask.TYPE);
    when(anyOtherTask.getTaskResource()).thenReturn(taskResource1);

    // Not a parallel index task ->  satisfies parallel index constraint
    // But does not satisfy the total slot constraint and cannot run (11 > 10)
    taskLimitsMap.put(IndexTask.TYPE, 0.5);
    Assert.assertFalse(workerInfo.canRunTask(anyOtherTask, taskLimitsMap));

    // Task has an availability conflict ("grp1")
    TaskResource taskResource2 = mock(TaskResource.class);
    when(taskResource2.getRequiredCapacity()).thenReturn(1);
    when(taskResource2.getAvailabilityGroup()).thenReturn(AVAILABILITY_GROUP);
    Task grp1Task = mock(IndexTask.class);
    when(grp1Task.getType()).thenReturn(RANDOM_TYPE);
    when(grp1Task.getTaskResource()).thenReturn(taskResource2);

    // Satisifies parallel index and total index slot constraints but cannot run due availability
    taskLimitsMap.put(RANDOM_TYPE, 0.3);
    Assert.assertFalse(workerInfo.canRunTask(grp1Task, taskLimitsMap));

    TaskResource taskResource3 = mock(TaskResource.class);
    when(taskResource1.getRequiredCapacity()).thenReturn(1);

    Task msqControllerTask = mock(AbstractTask.class);
    when(msqControllerTask.getType()).thenReturn(MSQ_CONTROLLER_TYPE);
    when(msqControllerTask.getTaskResource()).thenReturn(taskResource3);

    Map<String, Integer> mockStatusMap = new HashMap<>();
    mockStatusMap.put(MSQ_CONTROLLER_TYPE, 4);

    ImmutableWorkerInfo spyInfo = Mockito.spy(workerInfo);
    when(spyInfo.getTypeSpecificCapacityMap()).thenReturn(mockStatusMap);
    when(taskResource3.getRequiredCapacity()).thenReturn(1);

    taskLimitsMap.put(MSQ_CONTROLLER_TYPE, 4);
    Assert.assertFalse(spyInfo.canRunTask(msqControllerTask, taskLimitsMap));
    taskLimitsMap.put(MSQ_CONTROLLER_TYPE, 5);
    Assert.assertTrue(spyInfo.canRunTask(msqControllerTask, taskLimitsMap));
  }

  private void assertEqualsAndHashCode(ImmutableWorkerInfo o1, ImmutableWorkerInfo o2, boolean shouldMatch)
  {
    if (shouldMatch) {
      Assert.assertEquals(o1, o2);
      Assert.assertEquals(o1.hashCode(), o2.hashCode());
    } else {
      Assert.assertNotEquals(o1, o2);
      Assert.assertNotEquals(o1.hashCode(), o2.hashCode());
    }
  }
}
