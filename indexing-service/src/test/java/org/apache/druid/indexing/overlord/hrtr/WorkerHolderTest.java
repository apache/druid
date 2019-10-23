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
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.WorkerHistoryItem;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.ChangeRequestHttpSyncer;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class WorkerHolderTest
{
  @Test
  public void testSyncListener()
  {
    List<TaskAnnouncement> updates = new ArrayList<>();

    Task task0 = NoopTask.create("task0", 0);
    Task task1 = NoopTask.create("task1", 0);
    Task task2 = NoopTask.create("task2", 0);
    Task task3 = NoopTask.create("task3", 0);

    WorkerHolder workerHolder = new WorkerHolder(
        TestHelper.makeJsonMapper(),
        EasyMock.createNiceMock(HttpClient.class),
        new HttpRemoteTaskRunnerConfig(),
        EasyMock.createNiceMock(ScheduledExecutorService.class),
        (taskAnnouncement, holder) -> updates.add(taskAnnouncement),
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

    Assert.assertTrue(workerHolder.disabled.get());

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

    Assert.assertFalse(workerHolder.disabled.get());

    Assert.assertEquals(4, updates.size());

    Assert.assertEquals(task1.getId(), updates.get(0).getTaskId());
    Assert.assertTrue(updates.get(0).getTaskStatus().isSuccess());

    Assert.assertEquals(task2.getId(), updates.get(1).getTaskId());
    Assert.assertTrue(updates.get(1).getTaskStatus().isRunnable());

    Assert.assertEquals(task3.getId(), updates.get(2).getTaskId());
    Assert.assertTrue(updates.get(2).getTaskStatus().isRunnable());

    Assert.assertEquals(task0.getId(), updates.get(3).getTaskId());
    Assert.assertTrue(updates.get(3).getTaskStatus().isFailure());

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

    Assert.assertFalse(workerHolder.disabled.get());
    Assert.assertEquals(2, updates.size());

    Assert.assertEquals(task2.getId(), updates.get(0).getTaskId());
    Assert.assertTrue(updates.get(0).getTaskStatus().isFailure());

    Assert.assertEquals(task3.getId(), updates.get(1).getTaskId());
    Assert.assertTrue(updates.get(1).getTaskStatus().isRunnable());

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

    Assert.assertTrue(workerHolder.disabled.get());

    Assert.assertEquals(3, updates.size());

    Assert.assertEquals(task1.getId(), updates.get(0).getTaskId());
    Assert.assertTrue(updates.get(0).getTaskStatus().isSuccess());

    Assert.assertEquals(task2.getId(), updates.get(1).getTaskId());
    Assert.assertTrue(updates.get(1).getTaskStatus().isRunnable());

    Assert.assertEquals(task3.getId(), updates.get(2).getTaskId());
    Assert.assertTrue(updates.get(2).getTaskStatus().isRunnable());

    updates.clear();
  }
}
