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

package org.apache.druid.indexing.overlord.hrtr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.WorkerHistoryItem;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.ChangeRequestHttpSyncer;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class WorkerHolderTest
{
  @Test
  public void testSyncListener()
  {
    List<TaskAnnouncement> updates = new ArrayList<>();

    Task task0 = NoopTask.create();
    Task task1 = NoopTask.create();
    Task task2 = NoopTask.create();
    Task task3 = NoopTask.create();

    WorkerHolder workerHolder = new WorkerHolder(
        TestHelper.makeJsonMapper(),
        EasyMock.createNiceMock(HttpClient.class),
        new HttpRemoteTaskRunnerConfig(),
        EasyMock.createNiceMock(ScheduledExecutorService.class),
        new WorkerHolder.Listener()
        {
          @Override
          public void taskAddedOrUpdated(TaskAnnouncement announcement, WorkerHolder workerHolder)
          {
            updates.add(announcement);
          }

          @Override
          public void stateChanged(boolean enabled, WorkerHolder workerHolder)
          {
            // Do nothing
          }
        },
        new Worker("http", "localhost", "127.0.0.1", 5, "v0", WorkerConfig.DEFAULT_CATEGORY),
        ImmutableList.of(
            TaskAnnouncement.create(
                task0,
                TaskStatus.running(task0.getId()),
                TaskLocation.unknown()
            ),
            TaskAnnouncement.create(
                task1,
                TaskStatus.running(task1.getId()),
                TaskLocation.unknown()
            )
        )
    );

    ChangeRequestHttpSyncer.Listener<WorkerHistoryItem> syncListener = workerHolder.createSyncListener();

    Assertions.assertFalse(workerHolder.isEnabled());
    Assertions.assertFalse(workerHolder.isInitialized());

    syncListener.fullSync(
        ImmutableList.of(
            new WorkerHistoryItem.Metadata(false),
            new WorkerHistoryItem.TaskUpdate(TaskAnnouncement.create(
                task1,
                TaskStatus.success(task1.getId()),
                TaskLocation.create("w1", 1, -1)
            )),
            new WorkerHistoryItem.TaskUpdate(TaskAnnouncement.create(
                task2,
                TaskStatus.running(task2.getId()),
                TaskLocation.create("w1", 2, -1)
            )),
            new WorkerHistoryItem.TaskUpdate(TaskAnnouncement.create(
                task3,
                TaskStatus.running(task3.getId()),
                TaskLocation.create("w1", 2, -1)
            ))
        )
    );

    Assertions.assertTrue(workerHolder.isEnabled());

    Assertions.assertEquals(4, updates.size());

    Assertions.assertEquals(task1.getId(), updates.get(0).getTaskId());
    Assertions.assertTrue(updates.get(0).getTaskStatus().isSuccess());

    Assertions.assertEquals(task2.getId(), updates.get(1).getTaskId());
    Assertions.assertTrue(updates.get(1).getTaskStatus().isRunnable());

    Assertions.assertEquals(task3.getId(), updates.get(2).getTaskId());
    Assertions.assertTrue(updates.get(2).getTaskStatus().isRunnable());

    Assertions.assertEquals(task0.getId(), updates.get(3).getTaskId());
    Assertions.assertTrue(updates.get(3).getTaskStatus().isFailure());
    Assertions.assertNotNull(updates.get(3).getTaskStatus().getErrorMsg());
    Assertions.assertTrue(
        updates.get(3).getTaskStatus().getErrorMsg().startsWith(
            "This task disappeared on the worker where it was assigned"
        )
    );

    updates.clear();

    syncListener.deltaSync(
        ImmutableList.of(
            new WorkerHistoryItem.Metadata(false),
            new WorkerHistoryItem.TaskRemoval(task1.getId()),
            new WorkerHistoryItem.Metadata(true),
            new WorkerHistoryItem.TaskRemoval(task2.getId()),
            new WorkerHistoryItem.Metadata(false),
            new WorkerHistoryItem.TaskUpdate(TaskAnnouncement.create(
                task3,
                TaskStatus.running(task3.getId()),
                TaskLocation.create("w1", 3, -1)
            ))
        )
    );

    Assertions.assertTrue(workerHolder.isEnabled());
    Assertions.assertEquals(2, updates.size());

    Assertions.assertEquals(task2.getId(), updates.get(0).getTaskId());
    Assertions.assertTrue(updates.get(0).getTaskStatus().isFailure());
    Assertions.assertNotNull(updates.get(0).getTaskStatus().getErrorMsg());
    Assertions.assertTrue(
        updates.get(0).getTaskStatus().getErrorMsg().startsWith(
            "This task disappeared on the worker where it was assigned"
        )
    );

    Assertions.assertEquals(task3.getId(), updates.get(1).getTaskId());
    Assertions.assertTrue(updates.get(1).getTaskStatus().isRunnable());

    updates.clear();

    syncListener.fullSync(
        ImmutableList.of(
            new WorkerHistoryItem.Metadata(true),
            new WorkerHistoryItem.TaskUpdate(TaskAnnouncement.create(
                task1,
                TaskStatus.success(task1.getId()),
                TaskLocation.create("w1", 1, -1)
            )),
            new WorkerHistoryItem.TaskUpdate(TaskAnnouncement.create(
                task2,
                TaskStatus.running(task2.getId()),
                TaskLocation.create("w1", 2, -1)
            )),
            new WorkerHistoryItem.TaskUpdate(TaskAnnouncement.create(
                task3,
                TaskStatus.running(task3.getId()),
                TaskLocation.create("w1", 2, -1)
            ))
        )
    );

    Assertions.assertFalse(workerHolder.isEnabled());

    Assertions.assertEquals(3, updates.size());

    Assertions.assertEquals(task1.getId(), updates.get(0).getTaskId());
    Assertions.assertTrue(updates.get(0).getTaskStatus().isSuccess());

    Assertions.assertEquals(task2.getId(), updates.get(1).getTaskId());
    Assertions.assertTrue(updates.get(1).getTaskStatus().isRunnable());

    Assertions.assertEquals(task3.getId(), updates.get(2).getTaskId());
    Assertions.assertTrue(updates.get(2).getTaskStatus().isRunnable());

    updates.clear();
  }

  @Test
  public void testToImmutableIsUpdatedBeforeListenerIsNotified()
  {
    final Task task0 = NoopTask.create();
    final Task task1 = NoopTask.create();
    final RunningTasksRecorder recorder = new RunningTasksRecorder();
    final WorkerHolder workerHolder = createWorkerHolder(recorder, running(task0));
    final ChangeRequestHttpSyncer.Listener<WorkerHistoryItem> syncListener = workerHolder.createSyncListener();

    // Known announcements are reflected before any sync
    Assertions.assertEquals(ImmutableSet.of(task0.getId()), workerHolder.toImmutable().getRunningTasks());

    syncListener.fullSync(
        ImmutableList.of(new WorkerHistoryItem.Metadata(false), update(running(task0)), update(running(task1)))
    );
    final Set<String> bothTasks = ImmutableSet.of(task0.getId(), task1.getId());
    Assertions.assertEquals(ImmutableList.of(bothTasks, bothTasks), recorder.drain());
    Assertions.assertFalse(workerHolder.toImmutable().getWorker().isDisabled());

    syncListener.deltaSync(ImmutableList.of(update(succeeded(task0))));
    Assertions.assertEquals(ImmutableList.of(ImmutableSet.of(task1.getId())), recorder.drain());

    syncListener.deltaSync(ImmutableList.of(new WorkerHistoryItem.TaskRemoval(task1.getId())));
    Assertions.assertEquals(ImmutableList.of(ImmutableSet.of()), recorder.drain());
    Assertions.assertEquals(0, workerHolder.toImmutable().getCurrCapacityUsed());
  }

  @Test
  public void testToImmutableReflectsFieldsNotDerivedFromAnnouncementsWithoutSync()
  {
    final WorkerHolder workerHolder = createWorkerHolder(new RunningTasksRecorder());
    final DateTime lastCompletedTaskTime = DateTimes.of("2026-01-01");
    final DateTime blacklistedUntil = DateTimes.of("2026-01-02");

    workerHolder.setLastCompletedTaskTime(lastCompletedTaskTime);
    workerHolder.setBlacklistedUntil(blacklistedUntil);

    Assertions.assertEquals(lastCompletedTaskTime, workerHolder.toImmutable().getLastCompletedTaskTime());
    Assertions.assertEquals(blacklistedUntil, workerHolder.toImmutable().getBlacklistedUntil());
    // Disabled until the first sync enables it
    Assertions.assertTrue(workerHolder.toImmutable().getWorker().isDisabled());
  }

  @Test
  public void testWorkerRejoiningWithPlaceholderAnnouncements()
  {
    final String taskId = "task1";
    final TaskAnnouncement placeholder = TaskAnnouncement.create(
        taskId,
        NoopTask.TYPE,
        null,
        TaskStatus.running(taskId),
        TaskLocation.unknown(),
        null
    );
    final WorkerHolder workerHolder = createWorkerHolder(new RunningTasksRecorder(), placeholder);

    Assertions.assertEquals(ImmutableSet.of(taskId), workerHolder.toImmutable().getRunningTasks());
    Assertions.assertEquals(1, workerHolder.toImmutable().getCurrCapacityUsed());
  }

  @Test
  public void testToImmutableDoesNotExposeCachedCapacityByTaskType()
  {
    final Task task = NoopTask.create();
    final WorkerHolder workerHolder = createWorkerHolder(new RunningTasksRecorder(), running(task));
    final Map<String, Integer> capacityByTaskType = workerHolder.toImmutable().getCurrCapacityUsedByTaskType();

    Assertions.assertThrows(UnsupportedOperationException.class, () -> capacityByTaskType.put(task.getType(), 100));
    Assertions.assertEquals(
        ImmutableMap.of(task.getType(), 1),
        workerHolder.toImmutable().getCurrCapacityUsedByTaskType()
    );
  }

  private static WorkerHolder createWorkerHolder(WorkerHolder.Listener listener, TaskAnnouncement... knownAnnouncements)
  {
    return new WorkerHolder(
        TestHelper.makeJsonMapper(),
        EasyMock.createNiceMock(HttpClient.class),
        new HttpRemoteTaskRunnerConfig(),
        EasyMock.createNiceMock(ScheduledExecutorService.class),
        listener,
        new Worker("http", "localhost", "127.0.0.1", 5, "v1", WorkerConfig.DEFAULT_CATEGORY),
        ImmutableList.copyOf(knownAnnouncements)
    );
  }

  private static TaskAnnouncement running(Task task)
  {
    return TaskAnnouncement.create(task, TaskStatus.running(task.getId()), TaskLocation.unknown());
  }

  private static TaskAnnouncement succeeded(Task task)
  {
    return TaskAnnouncement.create(task, TaskStatus.success(task.getId()), TaskLocation.unknown());
  }

  private static WorkerHistoryItem update(TaskAnnouncement announcement)
  {
    return new WorkerHistoryItem.TaskUpdate(announcement);
  }

  /**
   * Records the running tasks seen through {@link WorkerHolder#toImmutable()} from inside each listener callback.
   */
  private static class RunningTasksRecorder implements WorkerHolder.Listener
  {
    private final List<Set<String>> seen = new ArrayList<>();

    @Override
    public void taskAddedOrUpdated(TaskAnnouncement announcement, WorkerHolder workerHolder)
    {
      seen.add(workerHolder.toImmutable().getRunningTasks());
    }

    @Override
    public void stateChanged(boolean enabled, WorkerHolder workerHolder)
    {
      // Do nothing
    }

    List<Set<String>> drain()
    {
      final List<Set<String>> drained = new ArrayList<>(seen);
      seen.clear();
      return drained;
    }
  }
}
