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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
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
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.ChangeRequestHttpSyncer;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
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
    Assert.assertNotNull(updates.get(3).getTaskStatus().getErrorMsg());
    Assert.assertTrue(
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

    Assert.assertFalse(workerHolder.disabled.get());
    Assert.assertEquals(2, updates.size());

    Assert.assertEquals(task2.getId(), updates.get(0).getTaskId());
    Assert.assertTrue(updates.get(0).getTaskStatus().isFailure());
    Assert.assertNotNull(updates.get(0).getTaskStatus().getErrorMsg());
    Assert.assertTrue(
        updates.get(0).getTaskStatus().getErrorMsg().startsWith(
            "This task disappeared on the worker where it was assigned"
        )
    );

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

  @Test
  public void testAssign() throws Exception
  {
    final Capture<Request> capturedRequest = EasyMock.newCapture();
    final HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject())).andReturn(
        Futures.immediateFuture(new StatusResponseHolder(HttpResponseStatus.NOT_FOUND, "not found"))
    ).once();
    EasyMock.expect(httpClient.go(EasyMock.capture(capturedRequest), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(
        Futures.immediateFuture(new StatusResponseHolder(HttpResponseStatus.OK, "ok"))
    ).once();
    EasyMock.replay(httpClient);

    final ObjectMapper smileMapper = TestHelper.makeJsonMapper();
    WorkerHolder workerHolder = new WorkerHolder(
        smileMapper,
        httpClient,
        new HttpRemoteTaskRunnerConfig(),
        EasyMock.createNiceMock(ScheduledExecutorService.class),
        (taskAnnouncement, holder) -> {},
        new Worker("http", "localhost", "127.0.0.1", 5, "v0", WorkerConfig.DEFAULT_CATEGORY),
        ImmutableList.of()
    );

    ChangeRequestHttpSyncer.Listener<WorkerHistoryItem> syncListener = workerHolder.createSyncListener();

    syncListener.fullSync(ImmutableList.of(
        new WorkerHistoryItem.Metadata(false)
        ));

    Assert.assertTrue(workerHolder.assignTask(NoopTask.create("task2", 0)));
    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());

    Assert.assertEquals(new URI("http://localhost/druid-internal/v1/worker/assignTask"),
                        capturedRequest.getValue().getUrl().toURI());
    Assert.assertEquals(NoopTask.create("task2", 0),
                        smileMapper.readValue(capturedRequest.getValue().getContent().array(), Task.class));
  }

  @Test
  public void testShutdown() throws Exception
  {
    final HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    final Capture<Request> capturedRequest = EasyMock.newCapture();
    EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject())).andReturn(
        Futures.immediateFuture(new StatusResponseHolder(HttpResponseStatus.NOT_FOUND, "not found"))
    ).once();
    EasyMock.expect(httpClient.go(EasyMock.capture(capturedRequest), EasyMock.anyObject(), EasyMock.anyObject())).andReturn(
        Futures.immediateFuture(new StatusResponseHolder(HttpResponseStatus.OK, "ok"))
    ).once();
    EasyMock.replay(httpClient);

    WorkerHolder workerHolder = new WorkerHolder(
        TestHelper.makeJsonMapper(),
        httpClient,
        new HttpRemoteTaskRunnerConfig(),
        EasyMock.createNiceMock(ScheduledExecutorService.class),
        (taskAnnouncement, holder) -> {},
        new Worker("http", "localhost", "127.0.0.1", 5, "v0", WorkerConfig.DEFAULT_CATEGORY),
        ImmutableList.of()
    );

    workerHolder.shutdownTask("task0");
    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(new URI("http://localhost/druid/worker/v1/task/task0/shutdown"), capturedRequest.getValue().getUrl().toURI());
  }
}
