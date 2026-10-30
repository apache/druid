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
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DurableStorageCleanerTest
{
  private static final TaskStorage TASK_STORAGE = Mockito.mock(TaskStorage.class);
  private static final TaskMaster TASK_MASTER = Mockito.mock(TaskMaster.class);
  private static final TaskRunner TASK_RUNNER = Mockito.mock(TaskRunner.class);
  private static final StorageConnector STORAGE_CONNECTOR = Mockito.mock(StorageConnector.class);
  private static final TaskRunnerWorkItem TASK_RUNNER_WORK_ITEM = Mockito.mock(TaskRunnerWorkItem.class);
  private static final String TASK_ID = "dummyTaskId";
  private static final String STRAY_DIR = "strayDirectory";
  private DurableStorageCleaner durableStorageCleaner;

  @BeforeEach
  public void setUp()
  {
    Mockito.reset(TASK_STORAGE, TASK_RUNNER, TASK_RUNNER_WORK_ITEM, STORAGE_CONNECTOR, TASK_MASTER);
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
    Mockito.when(TASK_STORAGE.getCompletedTasksInfo(ArgumentMatchers.any(), ArgumentMatchers.isNull())).thenReturn(List.of());
    Mockito.when(STORAGE_CONNECTOR.listDir(ArgumentMatchers.anyString())).thenReturn(
        ImmutableList.of(DurableStorageUtils.getControllerDirectory(TASK_ID), STRAY_DIR).iterator()
    );
    Mockito.when(TASK_RUNNER_WORK_ITEM.getTaskId()).thenReturn(TASK_ID);
    Mockito.doReturn(ImmutableList.of(TASK_RUNNER_WORK_ITEM)).when(TASK_RUNNER).getRunningTasks();
    Mockito.when(TASK_MASTER.getTaskRunner()).thenReturn(Optional.of(TASK_RUNNER));
    final ArgumentCaptor<Set<String>> capturedArguments = ArgumentCaptor.captor();

    durableStorageCleaner.run();

    Mockito.verify(STORAGE_CONNECTOR).deleteFiles(capturedArguments.capture());
    Assertions.assertEquals(Sets.newHashSet(STRAY_DIR), capturedArguments.getValue());
  }

  @Test
  public void testRunClearsStaleOrNotFoundTask() throws Exception
  {
    Mockito.when(TASK_STORAGE.getCompletedTasksInfo(ArgumentMatchers.any(), ArgumentMatchers.isNull())).thenReturn(List.of());
    Mockito.when(STORAGE_CONNECTOR.listDir(ArgumentMatchers.anyString())).thenReturn(
        ImmutableList.of(DurableStorageUtils.getControllerDirectory(TASK_ID)).iterator()
    );
    Mockito.when(TASK_RUNNER_WORK_ITEM.getTaskId()).thenReturn(TASK_ID);
    Mockito.doReturn(ImmutableList.of()).when(TASK_RUNNER).getRunningTasks();
    Mockito.when(TASK_MASTER.getTaskRunner()).thenReturn(Optional.of(TASK_RUNNER));
    final ArgumentCaptor<Set<String>> capturedArguments = ArgumentCaptor.captor();

    durableStorageCleaner.run();

    Mockito.verify(STORAGE_CONNECTOR).deleteFiles(capturedArguments.capture());
    Assertions.assertEquals(Set.of(DurableStorageUtils.getControllerDirectory(TASK_ID)), capturedArguments.getValue());
  }

  @Test
  public void testRunExcludesQueryDirectory() throws Exception
  {
    Task completedTask = new NoopTask(TASK_ID, null, null, 1, 0, null);
    Mockito.when(TASK_STORAGE.getCompletedTasksInfo(ArgumentMatchers.any(), ArgumentMatchers.isNull())).thenReturn(
        List.of(new TaskInfo(DateTimes.of("2020-01-01"), TaskStatus.success("not-used"), completedTask))
    );
    final String resultPath = DurableStorageUtils.QUERY_RESULTS_DIR + "/" + DurableStorageUtils.getControllerDirectory(
        TASK_ID) + "/results.json";
    final String intermediateFilesPath = DurableStorageUtils.getControllerDirectory(TASK_ID) + "/intermediate.frame";
    Mockito.when(STORAGE_CONNECTOR.listDir(ArgumentMatchers.anyString())).thenReturn(
        ImmutableList.of(resultPath, STRAY_DIR, intermediateFilesPath).iterator()
    );
    Mockito.when(TASK_MASTER.getTaskRunner()).thenReturn(Optional.of(TASK_RUNNER));
    Mockito.when(TASK_RUNNER_WORK_ITEM.getTaskId()).thenReturn(TASK_ID);
    Mockito.doReturn(ImmutableList.of()).when(TASK_RUNNER).getRunningTasks();
    final ArgumentCaptor<Set<String>> capturedArguments = ArgumentCaptor.captor();

    durableStorageCleaner.run();

    Mockito.verify(STORAGE_CONNECTOR).deleteFiles(capturedArguments.capture());
    Assertions.assertEquals(Sets.newHashSet(STRAY_DIR, intermediateFilesPath), capturedArguments.getValue());
  }

  @Test
  public void testRunDeletesInBatchesAndKeepsTheRemainder() throws Exception
  {
    final List<String> strayFiles = new ArrayList<>();
    for (int i = 0; i < 10_001; i++) {
      strayFiles.add(STRAY_DIR + i);
    }

    final List<String> listing = ImmutableList.<String>builder()
        .addAll(strayFiles)
        .add(DurableStorageUtils.getControllerDirectory(TASK_ID))
        .build();

    Mockito.when(TASK_STORAGE.getCompletedTasksInfo(ArgumentMatchers.any(), ArgumentMatchers.isNull())).thenReturn(List.of());
    Mockito.when(STORAGE_CONNECTOR.listDir(ArgumentMatchers.anyString())).thenReturn(listing.iterator());
    Mockito.when(TASK_RUNNER_WORK_ITEM.getTaskId()).thenReturn(TASK_ID);
    Mockito.doReturn(ImmutableList.of(TASK_RUNNER_WORK_ITEM)).when(TASK_RUNNER).getRunningTasks();
    Mockito.when(TASK_MASTER.getTaskRunner()).thenReturn(Optional.of(TASK_RUNNER));

    final Set<String> deleted = new HashSet<>();
    final List<Integer> batchSizes = new ArrayList<>();
    Mockito.doAnswer(invocation -> {
      final Iterable<String> batch = invocation.getArgument(0);
      int size = 0;
      for (String file : batch) {
        deleted.add(file);
        size++;
      }
      batchSizes.add(size);
      return null;
    }).when(STORAGE_CONNECTOR).deleteFiles(ArgumentMatchers.any());

    durableStorageCleaner.run();

    Assertions.assertTrue(batchSizes.size() > 1, "expected more than one batch, got " + batchSizes);
    Assertions.assertEquals(Sets.newHashSet(strayFiles), deleted);
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
    Assertions.assertEquals(cleanerConfig.delaySeconds * 1000, schedule.getPeriodMillis());
    Assertions.assertEquals(cleanerConfig.delaySeconds * 1000, schedule.getInitialDelayMillis());
  }
}
