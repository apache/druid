/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.hrtr;

import com.google.common.collect.ImmutableList;
import com.metamx.http.client.HttpClient;
import io.druid.indexer.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import io.druid.indexing.worker.TaskAnnouncement;
import io.druid.indexing.worker.Worker;
import io.druid.indexing.worker.WorkerHistoryItem;
import io.druid.segment.TestHelper;
import io.druid.server.coordination.ChangeRequestHttpSyncer;
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

    WorkerHolder workerHolder = new WorkerHolder(
        TestHelper.makeJsonMapper(),
        EasyMock.createNiceMock(HttpClient.class),
        new HttpRemoteTaskRunnerConfig(),
        EasyMock.createNiceMock(ScheduledExecutorService.class),
        (taskAnnouncement, holder) -> updates.add(taskAnnouncement),
        new Worker("http", "localhost", "127.0.0.1", 5, "v0")
    );

    ChangeRequestHttpSyncer.Listener<WorkerHistoryItem> syncListener = workerHolder.createSyncListener();

    Assert.assertTrue(workerHolder.disabled.get());

    Task task1 = NoopTask.create("task1", 0);
    Task task2 = NoopTask.create("task2", 0);
    Task task3 = NoopTask.create("task3", 0);

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

    Assert.assertEquals(3, updates.size());

    Assert.assertEquals(task1.getId(), updates.get(0).getTaskId());
    Assert.assertTrue(updates.get(0).getTaskStatus().isSuccess());

    Assert.assertEquals(task2.getId(), updates.get(1).getTaskId());
    Assert.assertTrue(updates.get(1).getTaskStatus().isRunnable());

    Assert.assertEquals(task3.getId(), updates.get(2).getTaskId());
    Assert.assertTrue(updates.get(2).getTaskStatus().isRunnable());

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
