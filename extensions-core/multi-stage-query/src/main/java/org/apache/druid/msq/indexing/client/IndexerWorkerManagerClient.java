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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.msq.exec.WorkerManagerClient;
import org.apache.druid.msq.indexing.MSQWorkerTask;
import org.apache.druid.rpc.indexing.OverlordClient;

import java.util.Map;
import java.util.Set;

/**
 * Worker manager client backed by the Indexer service. Glues together
 * three different mechanisms to provide the single multi-stage query interface.
 */
public class IndexerWorkerManagerClient implements WorkerManagerClient
{
  private final OverlordClient overlordClient;

  public IndexerWorkerManagerClient(final OverlordClient overlordClient)
  {
    this.overlordClient = overlordClient;
  }

  @Override
  public String run(String taskId, MSQWorkerTask task)
  {
    FutureUtils.getUnchecked(overlordClient.runTask(taskId, task), true);
    return taskId;
  }

  @Override
  public void cancel(String taskId)
  {
    FutureUtils.getUnchecked(overlordClient.cancelTask(taskId), true);
  }

  @Override
  public Map<String, TaskStatus> statuses(Set<String> taskIds)
  {
    return FutureUtils.getUnchecked(overlordClient.taskStatuses(taskIds), true);
  }

  @Override
  public TaskLocation location(String workerId)
  {
    final TaskStatus response = FutureUtils.getUnchecked(
        overlordClient.taskStatuses(ImmutableSet.of(workerId)),
        true
    ).get(workerId);

    if (response != null) {
      return response.getLocation();
    } else {
      return TaskLocation.unknown();
    }
  }

  @Override
  public void close()
  {
    // Nothing to do. The OverlordServiceClient is closed by the JVM lifecycle.
  }
}
