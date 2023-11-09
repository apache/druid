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
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UpdateLocationActionTest
{
  @Test
  public void testFlow() throws UnknownHostException
  {
    // get my task location
    InetAddress hostName = InetAddress.getLocalHost();
    TaskLocation myLocation = TaskLocation.create(hostName.getHostAddress(), 1, 2);
    UpdateLocationAction action = new UpdateLocationAction(myLocation);
    Task task = NoopTask.create();
    TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    TaskRunner runner = mock(TaskRunner.class);
    when(toolbox.getTaskRunner()).thenReturn(Optional.of(runner));
    action.perform(task, toolbox);
    verify(runner, times(1)).updateLocation(eq(task), eq(myLocation));
  }

  @Test
  public void testWithNoTaskRunner() throws UnknownHostException
  {
    // get my task location
    InetAddress hostName = InetAddress.getLocalHost();
    TaskLocation myLocation = TaskLocation.create(hostName.getHostAddress(), 1, 2);
    UpdateLocationAction action = new UpdateLocationAction(myLocation);
    Task task = NoopTask.create();
    TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    TaskRunner runner = mock(TaskRunner.class);
    when(toolbox.getTaskRunner()).thenReturn(Optional.absent());
    action.perform(task, toolbox);
    verify(runner, never()).updateStatus(any(), any());
  }
}
