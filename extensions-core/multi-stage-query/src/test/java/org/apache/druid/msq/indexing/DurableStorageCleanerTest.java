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

package org.apache.druid.msq.indexing;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.msq.shuffle.DurableStorageUtils;
import org.apache.druid.storage.StorageConnector;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Executors;


public class DurableStorageCleanerTest
{

  private static final TaskMaster TASK_MASTER = EasyMock.mock(TaskMaster.class);
  private static final TaskRunner TASK_RUNNER = EasyMock.mock(TaskRunner.class);
  private static final StorageConnector STORAGE_CONNECTOR = EasyMock.mock(StorageConnector.class);
  private static final TaskRunnerWorkItem TASK_RUNNER_WORK_ITEM = EasyMock.mock(TaskRunnerWorkItem.class);
  private static final TaskLocation TASK_LOCATION = new TaskLocation("dummy", 1000, -1);
  private static final String TASK_ID = "dummyTaskId";
  private static final String STRAY_DIR = "strayDirectory";

  @Test
  public void testSchedule() throws IOException, InterruptedException
  {
    EasyMock.reset(TASK_RUNNER, TASK_RUNNER_WORK_ITEM, STORAGE_CONNECTOR);
    DurableStorageCleanerConfig durableStorageCleanerConfig = new DurableStorageCleanerConfig();
    durableStorageCleanerConfig.delaySeconds = 1L;
    durableStorageCleanerConfig.enabled = true;
    DurableStorageCleaner durableStorageCleaner = new DurableStorageCleaner(
        durableStorageCleanerConfig,
        STORAGE_CONNECTOR,
        () -> TASK_MASTER
    );
    EasyMock.expect(STORAGE_CONNECTOR.listDir(EasyMock.anyString()))
            .andReturn(ImmutableList.of(DurableStorageUtils.getControllerDirectory(TASK_ID), "strayDirectory"))
            .anyTimes();
    EasyMock.expect(TASK_RUNNER_WORK_ITEM.getTaskId()).andReturn(TASK_ID)
            .anyTimes();
    EasyMock.expect((Collection<TaskRunnerWorkItem>) TASK_RUNNER.getRunningTasks())
            .andReturn(ImmutableList.of(TASK_RUNNER_WORK_ITEM))
            .anyTimes();
    EasyMock.expect(TASK_MASTER.getTaskRunner()).andReturn(Optional.of(TASK_RUNNER)).anyTimes();
    Capture<String> capturedArguments = EasyMock.newCapture();
    STORAGE_CONNECTOR.deleteRecursively(EasyMock.capture(capturedArguments));
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(TASK_MASTER, TASK_RUNNER, TASK_RUNNER_WORK_ITEM, STORAGE_CONNECTOR);


    durableStorageCleaner.schedule(Executors.newSingleThreadScheduledExecutor());
    Thread.sleep(8000L);
    Assert.assertEquals(STRAY_DIR, capturedArguments.getValue());
  }
}
