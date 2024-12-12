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

package org.apache.druid.indexing.common.actions;


import com.google.common.base.Optional;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UpdateStatusActionTest
{

  private static final String ID = "id";

  @Test
  public void testActionCallsTaskRunner()
  {
    UpdateStatusAction action = new UpdateStatusAction("successful");
    Task task = NoopTask.create();
    TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    TaskRunner runner = mock(TaskRunner.class);
    when(toolbox.getTaskRunner()).thenReturn(Optional.of(runner));
    action.perform(task, toolbox);
    verify(runner, times(1)).updateStatus(eq(task), eq(TaskStatus.success(task.getId())));
  }

  @Test
  public void testFailureScenario()
  {
    UpdateStatusAction action = new UpdateStatusAction("failure");
    Task task = NoopTask.create();
    TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    TaskRunner runner = mock(TaskRunner.class);
    when(toolbox.getTaskRunner()).thenReturn(Optional.of(runner));
    action.perform(task, toolbox);
    verify(runner, times(1)).updateStatus(eq(task), eq(TaskStatus.failure(task.getId(), "Error with task")));
  }

  @Test
  public void testTaskStatusFull()
  {
    Task task = NoopTask.create();
    TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    TaskRunner runner = mock(TaskRunner.class);
    when(toolbox.getTaskRunner()).thenReturn(Optional.of(runner));
    TaskStatus taskStatus = TaskStatus.failure(task.getId(), "custom error message");
    UpdateStatusAction action = new UpdateStatusAction("failure", taskStatus);
    action.perform(task, toolbox);
    verify(runner, times(1)).updateStatus(eq(task), eq(taskStatus));
  }

  @Test
  public void testNoTaskRunner()
  {
    UpdateStatusAction action = new UpdateStatusAction("", TaskStatus.success(ID));
    Task task = NoopTask.create();
    TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    TaskRunner runner = mock(TaskRunner.class);
    when(toolbox.getTaskRunner()).thenReturn(Optional.absent());
    action.perform(task, toolbox);
    verify(runner, never()).updateStatus(any(), any());
  }

  @Test
  public void testEquals()
  {
    UpdateStatusAction one = new UpdateStatusAction("", TaskStatus.failure(ID, "error"));
    UpdateStatusAction two = new UpdateStatusAction("", TaskStatus.failure(ID, "error"));
    Assert.assertEquals(one, two);
  }
}
