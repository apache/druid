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
import com.google.common.collect.Sets;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.duty.DutySchedule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.msq.indexing.cleaner.DurableStorageCleaner;
import org.apache.druid.msq.indexing.cleaner.DurableStorageCleanerConfig;
import org.apache.druid.storage.NilStorageConnector;
import org.apache.druid.storage.StorageConnector;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class DurableStorageCleanerTest
{
  private static final TaskStorage TASK_STORAGE = EasyMock.mock(TaskStorage.class);
  private static final TaskMaster TASK_MASTER = EasyMock.mock(TaskMaster.class);
  private static final TaskRunner TASK_RUNNER = EasyMock.mock(TaskRunner.class);
  private static final StorageConnector STORAGE_CONNECTOR = EasyMock.mock(StorageConnector.class);
  private static final TaskRunnerWorkItem TASK_RUNNER_WORK_ITEM = EasyMock.mock(TaskRunnerWorkItem.class);
  private static final String TASK_ID = "dummyTaskId";
  private static final String STRAY_DIR = "strayDirectory";
  private DurableStorageCleaner durableStorageCleaner;

  @Before
  public void setUp()
  {
    EasyMock.reset(TASK_STORAGE, TASK_RUNNER, TASK_RUNNER_WORK_ITEM, STORAGE_CONNECTOR, TASK_MASTER);
    DurableStorageCleanerConfig durableStorageCleanerConfig = new DurableStorageCleanerConfig();
    durableStorageCleanerConfig.delaySeconds = 1L;
    durableStorageCleanerConfig.enabled = true;
    durableStorageCleanerConfig.durationToRetain = new Duration(5_000L);
    durableStorageCleaner = new DurableStorageCleaner(
        durableStorageCleanerConfig,
        s -> STORAGE_CONNECTOR,
        () -> TASK_MASTER,
        TASK_STORAGE
    );
  }

  @Test
  public void testRun() throws Exception
  {
    EasyMock.expect(TASK_STORAGE.getCompletedTasksInfo(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(List.of())
            .anyTimes();
    EasyMock.expect(STORAGE_CONNECTOR.listDir(EasyMock.anyString()))
            .andReturn(ImmutableList.of(DurableStorageUtils.getControllerDirectory(TASK_ID), STRAY_DIR)
                                    .stream()
                                    .iterator())
            .anyTimes();
    EasyMock.expect(TASK_RUNNER_WORK_ITEM.getTaskId()).andReturn(TASK_ID)
            .anyTimes();
    EasyMock.expect((Collection<TaskRunnerWorkItem>) TASK_RUNNER.getRunningTasks())
            .andReturn(ImmutableList.of(TASK_RUNNER_WORK_ITEM))
            .anyTimes();
    EasyMock.expect(TASK_MASTER.getTaskRunner()).andReturn(Optional.of(TASK_RUNNER)).anyTimes();
    Capture<Set<String>> capturedArguments = EasyMock.newCapture();
    STORAGE_CONNECTOR.deleteFiles(EasyMock.capture(capturedArguments));
    EasyMock.expectLastCall().once();
    EasyMock.replay(TASK_STORAGE, TASK_MASTER, TASK_RUNNER, TASK_RUNNER_WORK_ITEM, STORAGE_CONNECTOR);

    durableStorageCleaner.run();

    Assert.assertEquals(Sets.newHashSet(STRAY_DIR), capturedArguments.getValue());
  }

  @Test
  public void testRunClearsStaleOrNotFoundTask() throws Exception
  {
    EasyMock.expect(TASK_STORAGE.getCompletedTasksInfo(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(List.of())
            .anyTimes();
    EasyMock.expect(STORAGE_CONNECTOR.listDir(EasyMock.anyString()))
            .andReturn(ImmutableList.of(DurableStorageUtils.getControllerDirectory(TASK_ID))
                                    .stream()
                                    .iterator())
            .anyTimes();
    EasyMock.expect(TASK_RUNNER_WORK_ITEM.getTaskId()).andReturn(TASK_ID)
            .anyTimes();
    EasyMock.expect((Collection<TaskRunnerWorkItem>) TASK_RUNNER.getRunningTasks())
            .andReturn(ImmutableList.of())
            .anyTimes();
    EasyMock.expect(TASK_MASTER.getTaskRunner()).andReturn(Optional.of(TASK_RUNNER)).anyTimes();
    Capture<Set<String>> capturedArguments = EasyMock.newCapture();
    STORAGE_CONNECTOR.deleteFiles(EasyMock.capture(capturedArguments));
    EasyMock.expectLastCall().once();
    EasyMock.replay(TASK_STORAGE, TASK_MASTER, TASK_RUNNER, TASK_RUNNER_WORK_ITEM, STORAGE_CONNECTOR);

    durableStorageCleaner.run();

    Assert.assertEquals(Set.of(DurableStorageUtils.getControllerDirectory(TASK_ID)), capturedArguments.getValue());
  }

  @Test
  public void testRunExcludesQueryDirectory() throws Exception
  {
    Task completedTask = new NoopTask(TASK_ID, null, null, 1, 0, null);
    EasyMock.expect(TASK_STORAGE.getCompletedTasksInfo(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(List.of(new TaskInfo(DateTimes.of("2020-01-01"), TaskStatus.success("not-used"), completedTask)))
            .anyTimes();
    final String resultPath = DurableStorageUtils.QUERY_RESULTS_DIR + "/" + DurableStorageUtils.getControllerDirectory(
        TASK_ID) + "/results.json";
    final String intermediateFilesPath = DurableStorageUtils.getControllerDirectory(TASK_ID) + "/intermediate.frame";
    EasyMock.expect(STORAGE_CONNECTOR.listDir(EasyMock.anyString()))
            .andReturn(ImmutableList.of(resultPath, STRAY_DIR, intermediateFilesPath)
                                    .stream()
                                    .iterator())
            .anyTimes();
    EasyMock.expect(TASK_MASTER.getTaskRunner()).andReturn(Optional.of(TASK_RUNNER)).anyTimes();
    EasyMock.expect(TASK_RUNNER_WORK_ITEM.getTaskId()).andReturn(TASK_ID)
            .anyTimes();
    EasyMock.expect((Collection<TaskRunnerWorkItem>) TASK_RUNNER.getRunningTasks())
            .andReturn(ImmutableList.of())
            .anyTimes();
    Capture<Set<String>> capturedArguments = EasyMock.newCapture();
    STORAGE_CONNECTOR.deleteFiles(EasyMock.capture(capturedArguments));
    EasyMock.expectLastCall().once();
    EasyMock.replay(TASK_STORAGE, TASK_MASTER, TASK_RUNNER, TASK_RUNNER_WORK_ITEM, STORAGE_CONNECTOR);

    durableStorageCleaner.run();

    Assert.assertEquals(Sets.newHashSet(STRAY_DIR, intermediateFilesPath), capturedArguments.getValue());
  }

  @Test
  public void testGetSchedule()
  {
    DurableStorageCleanerConfig cleanerConfig = new DurableStorageCleanerConfig();
    cleanerConfig.delaySeconds = 10L;
    cleanerConfig.enabled = true;
    DurableStorageCleaner durableStorageCleaner = new DurableStorageCleaner(
        cleanerConfig,
        (temp) -> NilStorageConnector.getInstance(),
        null,
        null
    );

    DutySchedule schedule = durableStorageCleaner.getSchedule();
    Assert.assertEquals(cleanerConfig.delaySeconds * 1000, schedule.getPeriodMillis());
    Assert.assertEquals(cleanerConfig.delaySeconds * 1000, schedule.getInitialDelayMillis());
  }
}
