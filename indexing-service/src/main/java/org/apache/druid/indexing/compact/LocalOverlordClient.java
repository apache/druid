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

package org.apache.druid.indexing.compact;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.http.TotalWorkerCapacityResponse;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Dummy Overlord client used by the {@link OverlordCompactionScheduler} to fetch
 * task related info. This client simply redirects all queries to the
 * {@link TaskQueryTool} and all updates to the {@link TaskQueue}.
 */
class LocalOverlordClient extends NoopOverlordClient
{
  private static final Logger log = new Logger(LocalOverlordClient.class);

  private final TaskMaster taskMaster;
  private final TaskQueryTool taskQueryTool;
  private final ObjectMapper objectMapper;

  LocalOverlordClient(TaskMaster taskMaster, TaskQueryTool taskQueryTool, ObjectMapper objectMapper)
  {
    this.taskMaster = taskMaster;
    this.taskQueryTool = taskQueryTool;
    this.objectMapper = objectMapper;
  }

  @Override
  public ListenableFuture<Void> runTask(String taskId, Object clientTaskQuery)
  {
    return futureOf(() -> {
      getValidTaskQueue().add(
          convertTask(clientTaskQuery, ClientCompactionTaskQuery.class, CompactionTask.class)
      );
      return null;
    });
  }

  @Override
  public ListenableFuture<Void> cancelTask(String taskId)
  {
    return futureOf(() -> {
      getValidTaskQueue().shutdown(taskId, "Shutdown by Compaction Scheduler");
      return null;
    });
  }

  @Override
  public ListenableFuture<TaskPayloadResponse> taskPayload(String taskId)
  {
    ClientCompactionTaskQuery taskPayload = taskQueryTool.getTask(taskId).transform(
        task -> convertTask(task, CompactionTask.class, ClientCompactionTaskQuery.class)
    ).orNull();
    return futureOf(() -> new TaskPayloadResponse(taskId, taskPayload));
  }

  @Override
  public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
      @Nullable String state,
      @Nullable String dataSource,
      @Nullable Integer maxCompletedTasks
  )
  {
    final ListenableFuture<List<TaskStatusPlus>> tasksFuture
        = futureOf(taskQueryTool::getAllActiveTasks);
    return Futures.transform(
        tasksFuture,
        taskList -> CloseableIterators.withEmptyBaggage(taskList.iterator()),
        Execs.directExecutor()
    );
  }

  @Override
  public ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds)
  {
    return futureOf(() -> taskQueryTool.getMultipleTaskStatuses(taskIds));
  }

  @Override
  public ListenableFuture<Map<String, List<Interval>>> findLockedIntervals(
      List<LockFilterPolicy> lockFilterPolicies
  )
  {
    return futureOf(() -> taskQueryTool.getLockedIntervals(lockFilterPolicies));
  }

  @Override
  public ListenableFuture<IndexingTotalWorkerCapacityInfo> getTotalWorkerCapacity()
  {
    return futureOf(() -> convert(taskQueryTool.getTotalWorkerCapacity()));
  }

  private TaskQueue getValidTaskQueue()
  {
    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      return taskQueue.get();
    } else {
      throw DruidException.defensive("No TaskQueue. Cannot proceed.");
    }
  }

  private <T> ListenableFuture<T> futureOf(Supplier<T> supplier)
  {
    try {
      return Futures.immediateFuture(supplier.get());
    }
    catch (Exception e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  private IndexingTotalWorkerCapacityInfo convert(TotalWorkerCapacityResponse capacity)
  {
    if (capacity == null) {
      return null;
    } else {
      return new IndexingTotalWorkerCapacityInfo(
          capacity.getCurrentClusterCapacity(),
          capacity.getMaximumCapacityWithAutoScale()
      );
    }
  }

  private <U, V> V convertTask(Object taskPayload, Class<U> inputType, Class<V> outputType)
  {
    if (taskPayload == null) {
      return null;
    } else if (!inputType.isInstance(taskPayload)) {
      throw DruidException.defensive(
          "Unknown type[%s] for compaction task. Expected type[%s].",
          taskPayload.getClass().getSimpleName(), inputType.getSimpleName()
      );
    }

    try {
      return objectMapper.readValue(
          objectMapper.writeValueAsBytes(taskPayload),
          outputType
      );
    }
    catch (IOException e) {
      log.warn(e, "Could not convert task[%s] to client compatible object", taskPayload);
      throw DruidException.defensive(
          "Could not convert task[%s] to compatible object.",
          taskPayload
      );
    }
  }
}
