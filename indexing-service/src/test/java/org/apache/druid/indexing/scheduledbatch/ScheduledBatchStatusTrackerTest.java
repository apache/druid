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

import com.google.common.collect.ImmutableList;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.query.http.SqlTaskStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

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
    final BatchSupervisorTaskReport report =
        statusTracker.getSupervisorTaskReport(SUPERVISOR_ID_1);

    assertNotNull(report);

    assertTrue(report.getRecentTasks().isEmpty());

    assertEquals(0, report.getTotalSubmittedTasks());
    assertEquals(0, report.getTotalSuccessfulTasks());
    assertEquals(0, report.getTotalFailedTasks());
  }

  @Test
  public void testOnTaskSubmitted()
  {
    final SqlTaskStatus sqlTaskStatus = new SqlTaskStatus(TASK_ID_1, TaskState.RUNNING, null);

    statusTracker.onTaskSubmitted(SUPERVISOR_ID_1, sqlTaskStatus);

    final BatchSupervisorTaskReport report = statusTracker.getSupervisorTaskReport(SUPERVISOR_ID_1);
    assertNotNull(report);

    verifyTaskStatus(report.getRecentTasks(), ImmutableList.of(TaskStatus.running(TASK_ID_1)));

    assertEquals(1, report.getTotalSubmittedTasks());
    assertEquals(0, report.getTotalSuccessfulTasks());
    assertEquals(0, report.getTotalFailedTasks());
  }

  @Test
  public void testOnTaskCompletedSuccessfully()
  {
    final SqlTaskStatus sqlTaskStatus = new SqlTaskStatus(TASK_ID_1, TaskState.RUNNING, null);
    statusTracker.onTaskSubmitted(SUPERVISOR_ID_1, sqlTaskStatus);

    statusTracker.onTaskCompleted(TASK_ID_1, TaskStatus.success(TASK_ID_1));

    final BatchSupervisorTaskReport report = statusTracker.getSupervisorTaskReport(SUPERVISOR_ID_1);
    assertNotNull(report);

    verifyTaskStatus(report.getRecentTasks(), ImmutableList.of(TaskStatus.success(TASK_ID_1)));

    assertEquals(1, report.getTotalSubmittedTasks());
    assertEquals(1, report.getTotalSuccessfulTasks());
    assertEquals(0, report.getTotalFailedTasks());
  }

  @Test
  public void testGetTasksWithMultipleTasks()
  {
    final SqlTaskStatus sqlTaskStatus1 = new SqlTaskStatus(TASK_ID_1, TaskState.RUNNING, null);
    final SqlTaskStatus sqlTaskStatus2 = new SqlTaskStatus(TASK_ID_2, TaskState.RUNNING, null);

    statusTracker.onTaskSubmitted(SUPERVISOR_ID_1, sqlTaskStatus1);
    statusTracker.onTaskSubmitted(SUPERVISOR_ID_1, sqlTaskStatus2);

    statusTracker.onTaskCompleted(TASK_ID_1, TaskStatus.success(TASK_ID_1));

    BatchSupervisorTaskReport report = statusTracker.getSupervisorTaskReport(SUPERVISOR_ID_1);
    assertNotNull(report);

    verifyTaskStatus(report.getRecentTasks(), ImmutableList.of(TaskStatus.success(TASK_ID_1), TaskStatus.running(TASK_ID_2)));

    statusTracker.onTaskCompleted(TASK_ID_2, TaskStatus.failure(TASK_ID_2, "some error message."));

    report = statusTracker.getSupervisorTaskReport(SUPERVISOR_ID_1);

    verifyTaskStatus(report.getRecentTasks(), ImmutableList.of(TaskStatus.success(TASK_ID_1), TaskStatus.failure(TASK_ID_2, "some error message.")));

    assertEquals(2, report.getTotalSubmittedTasks());
    assertEquals(1, report.getTotalSuccessfulTasks());
    assertEquals(1, report.getTotalFailedTasks());
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
    BatchSupervisorTaskReport report1 = statusTracker.getSupervisorTaskReport(SUPERVISOR_ID_1);
    assertNotNull(report1);

    verifyTaskStatus(report1.getRecentTasks(), ImmutableList.of(TaskStatus.running(TASK_ID_1), TaskStatus.running(TASK_ID_2)));

    // Verify the state of supervisor 2
    BatchSupervisorTaskReport report2 = statusTracker.getSupervisorTaskReport(SUPERVISOR_ID_2);
    assertNotNull(report2);

    verifyTaskStatus(report2.getRecentTasks(), ImmutableList.of(TaskStatus.success(TASK_ID_3)));


    // Complete tasks for supervisor 1
    statusTracker.onTaskCompleted(TASK_ID_1, TaskStatus.success(TASK_ID_1));
    statusTracker.onTaskCompleted(TASK_ID_2, TaskStatus.failure(TASK_ID_2, "Task failed"));

    // Verify final state of supervisor 1
    report1 = statusTracker.getSupervisorTaskReport(SUPERVISOR_ID_1);
    assertNotNull(report1);

    verifyTaskStatus(report1.getRecentTasks(), ImmutableList.of(TaskStatus.success(TASK_ID_1), TaskStatus.failure(TASK_ID_2, "Task failed")));

    // Verify total counts for both supervisors
    assertEquals(2, report1.getTotalSubmittedTasks());
    assertEquals(1, report1.getTotalSuccessfulTasks());
    assertEquals(1, report1.getTotalFailedTasks());

    assertEquals(1, report2.getTotalSubmittedTasks());
    assertEquals(1, report2.getTotalSuccessfulTasks());
    assertEquals(0, report2.getTotalFailedTasks());
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

    final BatchSupervisorTaskReport report1 = statusTracker.getSupervisorTaskReport(SUPERVISOR_ID_1);
    assertNotNull(report1);

    List<TaskStatus> sortedActualStatues = report1.getRecentTasks()
                                                  .stream()
                                                  .map(BatchSupervisorTaskStatus::getTaskStatus)
                                                  .sorted(Comparator.comparing(TaskStatus::getId))
                                                  .collect(Collectors.toList());
    Assert.assertEquals(
        sortedActualStatues,
        ImmutableList.of(TaskStatus.success(TASK_ID_1), TaskStatus.failure(TASK_ID_2, "Task failed"))
    );

    // Verify that tasks were correctly tracked for supervisor 2
    final BatchSupervisorTaskReport report2 = statusTracker.getSupervisorTaskReport(SUPERVISOR_ID_2);
    assertNotNull(report2);

    verifyTaskStatus(report2.getRecentTasks(), ImmutableList.of(TaskStatus.success(TASK_ID_3)));

    // Verify total counts for both supervisors
    assertEquals(2, report1.getTotalSubmittedTasks());
    assertEquals(1, report1.getTotalSuccessfulTasks());
    assertEquals(1, report1.getTotalFailedTasks());

    assertEquals(1, report2.getTotalSubmittedTasks());
    assertEquals(1, report2.getTotalSuccessfulTasks());
    assertEquals(0, report2.getTotalFailedTasks());

  }

  private void verifyTaskStatus(List<BatchSupervisorTaskStatus> actualStatues, List<TaskStatus> expectedStatuses)
  {
    assertEquals(expectedStatuses.size(), actualStatues.size());
    for (int i = 0; i < expectedStatuses.size(); i++) {
      assertEquals(expectedStatuses.get(i), actualStatues.get(i).getTaskStatus());
    }
  }
}
