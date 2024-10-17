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

  private ScheduledBatchStatusTracker statusTracker;

  @Before
  public void setUp()
  {
    statusTracker = new ScheduledBatchStatusTracker();
  }

  @Test
  public void testOnTaskSubmitted()
  {
    final String supervisorId = "supervisor1";
    final SqlTaskStatus sqlTaskStatus = new SqlTaskStatus("taskId1", TaskState.RUNNING, null);

    statusTracker.onTaskSubmitted(supervisorId, sqlTaskStatus);

    ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result = statusTracker.getSupervisorTasks(supervisorId);
    assertNotNull(result);
    assertEquals(ImmutableMap.of("taskId1", TaskStatus.running("taskId1")), result.getSubmittedTasks());
    assertTrue(result.getCompletedTasks().isEmpty());
  }

  @Test
  public void testOnTaskCompleted()
  {
    final String supervisorId = "supervisor1";
    final SqlTaskStatus sqlTaskStatus = new SqlTaskStatus("taskId1", TaskState.RUNNING, null);
    statusTracker.onTaskSubmitted(supervisorId, sqlTaskStatus);

    statusTracker.onTaskCompleted("taskId1", TaskStatus.success("taskId1"));

    final ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result =
        statusTracker.getSupervisorTasks(supervisorId);

    assertNotNull(result);
    assertTrue(result.getCompletedTasks().containsKey("taskId1"));
    assertEquals(TaskState.SUCCESS, result.getCompletedTasks().get("taskId1").getStatusCode());
    assertTrue(result.getSubmittedTasks().isEmpty());
  }

  @Test
  public void testGetTasksWithNoTasks()
  {
    final ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result =
        statusTracker.getSupervisorTasks("supervisor1");

    assertNotNull(result);
    assertTrue(result.getSubmittedTasks().isEmpty());
    assertTrue(result.getCompletedTasks().isEmpty());
  }

  @Test
  public void testGetTasksWithMultipleTasks()
  {
    final String supervisorId = "supervisor1";
    final SqlTaskStatus sqlTaskStatus1 = new SqlTaskStatus("taskId1", TaskState.RUNNING, null);
    final SqlTaskStatus sqlTaskStatus2 = new SqlTaskStatus("taskId2", TaskState.RUNNING, null);

    statusTracker.onTaskSubmitted(supervisorId, sqlTaskStatus1);
    statusTracker.onTaskSubmitted(supervisorId, sqlTaskStatus2);

    statusTracker.onTaskCompleted("taskId1", TaskStatus.success("taskId1"));

    final ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result =
        statusTracker.getSupervisorTasks(supervisorId);
    assertNotNull(result);
    assertTrue(result.getSubmittedTasks().containsKey("taskId2"));
    assertEquals(TaskState.RUNNING, result.getSubmittedTasks().get("taskId2").getStatusCode());

    assertTrue(result.getCompletedTasks().containsKey("taskId1"));
    assertEquals(TaskState.SUCCESS, result.getCompletedTasks().get("taskId1").getStatusCode());
  }

  @Test
  public void testThreadSafety() throws InterruptedException
  {
    final String supervisorId = "supervisor1";
    final SqlTaskStatus sqlTaskStatus1 = new SqlTaskStatus("taskId1", TaskState.RUNNING, null);
    final SqlTaskStatus sqlTaskStatus2 = new SqlTaskStatus("taskId2", TaskState.RUNNING, null);

    final Thread thread1 = new Thread(() -> {
      statusTracker.onTaskSubmitted(supervisorId, sqlTaskStatus1);
      statusTracker.onTaskCompleted("taskId1", TaskStatus.success("taskId1"));
    });
    final Thread thread2 = new Thread(() -> {
      statusTracker.onTaskSubmitted(supervisorId, sqlTaskStatus2);
      statusTracker.onTaskCompleted("taskId2", TaskStatus.success("taskId2"));
    });

    thread1.start();
    thread2.start();

    thread1.join();
    thread2.join();

    // Verify that both tasks were correctly tracked
    final ScheduledBatchStatusTracker.BatchSupervisorTaskStatus result = statusTracker.getSupervisorTasks(supervisorId);
    assertNotNull(result);
    assertEquals(2, result.getCompletedTasks().size());
    assertTrue(result.getCompletedTasks().containsKey("taskId1"));
    assertTrue(result.getCompletedTasks().containsKey("taskId2"));
  }
}
