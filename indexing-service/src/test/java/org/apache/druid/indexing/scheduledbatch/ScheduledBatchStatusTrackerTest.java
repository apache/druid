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

package org.apache.druid.indexing.scheduledbatch;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.sql.http.SqlTaskStatus;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ScheduledBatchStatusTrackerTest
{

  private static final String SUPERVISOR_ID_1 = "supervisor1";
  private static final String SUPERVISOR_ID_2 = "supervisor2";
  private static final String TASK_ID_1 = "taskId1";
  private static final String TASK_ID_2 = "taskId2";
  private static final String TASK_ID_3 = "taskId3";

  private ScheduledBatchStatusTracker statusTracker;

  @Before
  public void setUp()
  {
    statusTracker = new ScheduledBatchStatusTracker();
  }

  @Test
  public void testGetSupervisorTasksWithNoTasks()
  {
    final ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result =
        statusTracker.getSupervisorTasks(SUPERVISOR_ID_1);

    assertNotNull(result);
    assertTrue(result.getSubmittedTasks().isEmpty());
    assertTrue(result.getCompletedTasks().isEmpty());
  }

  @Test
  public void testOnTaskSubmitted()
  {
    final SqlTaskStatus sqlTaskStatus = new SqlTaskStatus(TASK_ID_1, TaskState.RUNNING, null);

    statusTracker.onTaskSubmitted(SUPERVISOR_ID_1, sqlTaskStatus);

    final ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result =
        statusTracker.getSupervisorTasks(SUPERVISOR_ID_1);
    assertNotNull(result);
    assertEquals(ImmutableMap.of(TASK_ID_1, TaskStatus.running(TASK_ID_1)), result.getSubmittedTasks());
    assertTrue(result.getCompletedTasks().isEmpty());
  }

  @Test
  public void testOnTaskCompleted()
  {
    final SqlTaskStatus sqlTaskStatus = new SqlTaskStatus(TASK_ID_1, TaskState.RUNNING, null);
    statusTracker.onTaskSubmitted(SUPERVISOR_ID_1, sqlTaskStatus);

    statusTracker.onTaskCompleted(TASK_ID_1, TaskStatus.success(TASK_ID_1));

    final ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result =
        statusTracker.getSupervisorTasks(SUPERVISOR_ID_1);

    assertNotNull(result);
    assertEquals(ImmutableMap.of(TASK_ID_1, TaskStatus.success(TASK_ID_1)), result.getCompletedTasks());
    assertTrue(result.getSubmittedTasks().isEmpty());
  }

  @Test
  public void testGetTasksWithMultipleTasks()
  {
    final SqlTaskStatus sqlTaskStatus1 = new SqlTaskStatus(TASK_ID_1, TaskState.RUNNING, null);
    final SqlTaskStatus sqlTaskStatus2 = new SqlTaskStatus(TASK_ID_2, TaskState.RUNNING, null);

    statusTracker.onTaskSubmitted(SUPERVISOR_ID_1, sqlTaskStatus1);
    statusTracker.onTaskSubmitted(SUPERVISOR_ID_1, sqlTaskStatus2);

    statusTracker.onTaskCompleted(TASK_ID_1, TaskStatus.success(TASK_ID_1));

    ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result =
        statusTracker.getSupervisorTasks(SUPERVISOR_ID_1);
    assertNotNull(result);
    assertEquals(ImmutableMap.of(TASK_ID_2, TaskStatus.running(TASK_ID_2)), result.getSubmittedTasks());
    assertEquals(ImmutableMap.of(TASK_ID_1, TaskStatus.success(TASK_ID_1)), result.getCompletedTasks());

    statusTracker.onTaskCompleted(TASK_ID_2, TaskStatus.failure(TASK_ID_2, "some error message."));

    result = statusTracker.getSupervisorTasks(SUPERVISOR_ID_1);
    assertEquals(ImmutableMap.of(), result.getSubmittedTasks());
    assertEquals(
        ImmutableMap.of(TASK_ID_1,
                        TaskStatus.success(TASK_ID_1),
                        TASK_ID_2,
                        TaskStatus.failure(TASK_ID_2, "some error message.")
        ),
        result.getCompletedTasks()
    );
  }

  @Test
  public void testMultipleSupervisors()
  {
    // Submit tasks for supervisor 1
    final SqlTaskStatus sqlTaskStatus1 = new SqlTaskStatus(TASK_ID_1, TaskState.RUNNING, null);
    final SqlTaskStatus sqlTaskStatus2 = new SqlTaskStatus(TASK_ID_2, TaskState.RUNNING, null);
    statusTracker.onTaskSubmitted(SUPERVISOR_ID_1, sqlTaskStatus1);
    statusTracker.onTaskSubmitted(SUPERVISOR_ID_1, sqlTaskStatus2);

    // Submit and complete a task for supervisor 2
    final SqlTaskStatus sqlTaskStatus3 = new SqlTaskStatus(TASK_ID_3, TaskState.RUNNING, null);
    statusTracker.onTaskSubmitted(SUPERVISOR_ID_2, sqlTaskStatus3);
    statusTracker.onTaskCompleted(TASK_ID_3, TaskStatus.success(TASK_ID_3));

    // Verify the state of supervisor 1
    ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result1 =
        statusTracker.getSupervisorTasks(SUPERVISOR_ID_1);
    assertNotNull(result1);
    assertEquals(ImmutableMap.of(TASK_ID_1, TaskStatus.running(TASK_ID_1), TASK_ID_2, TaskStatus.running(TASK_ID_2)),
                 result1.getSubmittedTasks()
    );
    assertTrue(result1.getCompletedTasks().isEmpty());

    // Verify the state of supervisor 2
    ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result2 =
        statusTracker.getSupervisorTasks(SUPERVISOR_ID_2);
    assertNotNull(result2);
    assertEquals(ImmutableMap.of(), result2.getSubmittedTasks());
    assertEquals(ImmutableMap.of(TASK_ID_3, TaskStatus.success(TASK_ID_3)), result2.getCompletedTasks());

    // Complete tasks for supervisor 1
    statusTracker.onTaskCompleted(TASK_ID_1, TaskStatus.success(TASK_ID_1));
    statusTracker.onTaskCompleted(TASK_ID_2, TaskStatus.failure(TASK_ID_2, "Task failed"));

    // Verify final state of supervisor 1
    result1 = statusTracker.getSupervisorTasks(SUPERVISOR_ID_1);
    assertNotNull(result1);
    assertEquals(ImmutableMap.of(), result1.getSubmittedTasks());
    assertEquals(ImmutableMap.of(TASK_ID_1,
                                 TaskStatus.success(TASK_ID_1),
                                 TASK_ID_2,
                                 TaskStatus.failure(TASK_ID_2, "Task failed")
                 ),
                 result1.getCompletedTasks()
    );
  }

  @Test
  public void testThreadSafety() throws InterruptedException
  {
    final SqlTaskStatus sqlTaskStatus1 = new SqlTaskStatus(TASK_ID_1, TaskState.RUNNING, null);
    final SqlTaskStatus sqlTaskStatus2 = new SqlTaskStatus(TASK_ID_2, TaskState.RUNNING, null);
    final SqlTaskStatus sqlTaskStatus3 = new SqlTaskStatus(TASK_ID_3, TaskState.RUNNING, null);

    final Thread thread1 = new Thread(() -> {
      statusTracker.onTaskSubmitted(SUPERVISOR_ID_1, sqlTaskStatus1);
      statusTracker.onTaskCompleted(TASK_ID_1, TaskStatus.success(TASK_ID_1));
    });
    final Thread thread2 = new Thread(() -> {
      statusTracker.onTaskSubmitted(SUPERVISOR_ID_1, sqlTaskStatus2);
      statusTracker.onTaskCompleted(TASK_ID_2, TaskStatus.failure(TASK_ID_2, "Task failed"));
    });
    final Thread thread3 = new Thread(() -> {
      statusTracker.onTaskSubmitted(SUPERVISOR_ID_2, sqlTaskStatus3);
      statusTracker.onTaskCompleted(TASK_ID_3, TaskStatus.success(TASK_ID_3));
    });

    thread1.start();
    thread2.start();
    thread3.start();

    thread1.join();
    thread2.join();
    thread3.join();

    final ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result1 =
        statusTracker.getSupervisorTasks(SUPERVISOR_ID_1);
    assertNotNull(result1);
    assertEquals(ImmutableMap.of(), result1.getSubmittedTasks());
    assertEquals(
        ImmutableMap.of(
            TASK_ID_1,
            TaskStatus.success(TASK_ID_1),
            TASK_ID_2,
            TaskStatus.failure(TASK_ID_2, "Task failed")
        ),
        result1.getCompletedTasks()
    );

    // Verify that tasks were correctly tracked for supervisor 2
    final ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result2 =
        statusTracker.getSupervisorTasks(SUPERVISOR_ID_2);
    assertNotNull(result2);
    assertEquals(ImmutableMap.of(), result2.getSubmittedTasks());
    assertEquals(ImmutableMap.of(TASK_ID_3, TaskStatus.success(TASK_ID_3)), result2.getCompletedTasks());
  }
}
