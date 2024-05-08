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

package org.apache.druid.msq.indexing.client;

import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;

public class IndexerWorkerManagerClientTest
{

  @Test
  public void testGetLocationCallsMultiStatusApiByDefault()
  {
    final OverlordClient overlordClient = Mockito.mock(OverlordClient.class);

    final String taskId = "worker1";
    final TaskLocation expectedLocation = new TaskLocation("localhost", 1000, 1100, null);
    Mockito.when(overlordClient.taskStatuses(Collections.singleton(taskId))).thenReturn(
        Futures.immediateFuture(
            Collections.singletonMap(
                taskId,
                new TaskStatus(taskId, TaskState.RUNNING, 100L, null, expectedLocation)
            )
        )
    );

    final IndexerWorkerManagerClient managerClient = new IndexerWorkerManagerClient(overlordClient);
    Assert.assertEquals(managerClient.location(taskId), expectedLocation);

    Mockito.verify(overlordClient, Mockito.times(1)).taskStatuses(ArgumentMatchers.anySet());
    Mockito.verify(overlordClient, Mockito.never()).taskStatus(ArgumentMatchers.anyString());
  }

  @Test
  public void testGetLocationFallsBackToSingleTaskApiIfLocationIsUnknown()
  {
    final OverlordClient overlordClient = Mockito.mock(OverlordClient.class);

    final String taskId = "worker1";
    Mockito.when(overlordClient.taskStatuses(Collections.singleton(taskId))).thenReturn(
        Futures.immediateFuture(
            Collections.singletonMap(
                taskId,
                new TaskStatus(taskId, TaskState.RUNNING, 100L, null, TaskLocation.unknown())
            )
        )
    );

    final TaskLocation expectedLocation = new TaskLocation("localhost", 1000, 1100, null);
    final TaskStatusPlus taskStatus = new TaskStatusPlus(
        taskId,
        null,
        null,
        DateTimes.nowUtc(),
        DateTimes.nowUtc(),
        TaskState.RUNNING,
        null,
        100L,
        expectedLocation,
        "wiki",
        null
    );

    Mockito.when(overlordClient.taskStatus(taskId)).thenReturn(
        Futures.immediateFuture(new TaskStatusResponse(taskId, taskStatus))
    );

    final IndexerWorkerManagerClient managerClient = new IndexerWorkerManagerClient(overlordClient);
    Assert.assertEquals(managerClient.location(taskId), expectedLocation);

    Mockito.verify(overlordClient, Mockito.times(1)).taskStatuses(ArgumentMatchers.anySet());
    Mockito.verify(overlordClient, Mockito.times(1)).taskStatus(ArgumentMatchers.anyString());
  }

}
