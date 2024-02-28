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

package org.apache.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.createMock;

public class WorkerTaskManagerMonitorTest
{
  private WorkerTaskManager workerTaskManager;
  private TaskRunner taskRunner;
  private ObjectMapper jsonMapper;
  private TaskConfig taskConfig;
  private OverlordClient overlordClient;
  private Task task;
  private WorkerTaskManager.TaskDetails taskDetails;
  private TaskAnnouncement taskAnnouncement;

  @Before
  public void setUp()
  {
    task = createMock(Task.class);
    EasyMock.expect(task.getDataSource()).andReturn("dummy_DS1");
    EasyMock.replay(task);
    taskDetails = createMock(WorkerTaskManager.TaskDetails.class);
    EasyMock.expect(taskDetails.getDataSource()).andReturn("dummy_DS2");
    EasyMock.replay(taskDetails);
    taskAnnouncement = createMock(TaskAnnouncement.class);
    EasyMock.expect(taskAnnouncement.getTaskDataSource()).andReturn("dummy_DS3");
    EasyMock.replay(taskAnnouncement);
    taskRunner = createMock(TaskRunner.class);
    taskConfig = createMock(TaskConfig.class);
    overlordClient = createMock(OverlordClient.class);
    workerTaskManager = new WorkerTaskManager(jsonMapper, taskRunner, taskConfig, overlordClient)
    {
      @Override
      public Map<String, TaskDetails> getRunningTasks()
      {
        Map<String, TaskDetails> runningTasks = new HashMap<>();
        runningTasks.put("taskId1", taskDetails);
        return runningTasks;
      }
      @Override
      public Map<String, TaskAnnouncement> getCompletedTasks()
      {
        Map<String, TaskAnnouncement> completedTasks = new HashMap<>();
        completedTasks.put("taskId2", taskAnnouncement);
        return completedTasks;
      }
      @Override
      public Map<String, Task> getAssignedTasks()
      {
        Map<String, Task> assignedTasks = new HashMap<>();
        assignedTasks.put("taskId3", task);
        return assignedTasks;
      }
    };
  }

  @Test
  public void testWorkerTaskManagerMonitor()
  {
    final WorkerTaskManagerMonitor monitor = new WorkerTaskManagerMonitor(workerTaskManager);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    Assert.assertEquals(3, emitter.getEvents().size());
    emitter.verifyValue("worker/task/running/count", 1);
    emitter.verifyValue("worker/task/assigned/count", 1);
    emitter.verifyValue("worker/task/completed/count", 1);
  }
}
