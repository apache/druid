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

package org.apache.druid.rpc.indexing;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.ClientKillUnusedSegmentsTaskQuery;
import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.IndexingWorkerInfo;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * High-level Overlord client.
 *
 * All methods return futures, enabling asynchronous logic. If you want a synchronous response, use
 * {@code FutureUtils.get} or {@code FutureUtils.getUnchecked}.
 *
 * Futures resolve to exceptions in the manner described by {@link org.apache.druid.rpc.ServiceClient#asyncRequest}.
 *
 * Typically acquired via Guice, where it is registered using {@link org.apache.druid.rpc.guice.ServiceClientModule}.
 */
public interface OverlordClient
{
  /**
   * Contact the Overlord that we believe to be the leader, and return the result of its
   * {@code /druid/indexer/v1/leader} API. This may be a different Overlord server than the one we contacted, if
   * a leader change happened since the last time we updated our sense of who the leader is.
   */
  ListenableFuture<URI> findCurrentLeader();

  /**
   * Run a task with the provided ID and payload. The payload must be convertible by an
   * {@link com.fasterxml.jackson.databind.ObjectMapper} into a Task object. This method does not take Task objects
   * directly, because Task is in the indexing-service package.
   *
   * @param taskId     task ID
   * @param taskObject task payload
   */
  ListenableFuture<Void> runTask(String taskId, Object taskObject);

  /**
   * Run a "kill" task for a particular datasource and interval. Shortcut to {@link #runTask(String, Object)}.
   * The kill task deletes all unused segment records from deep storage and the metadata store. The task runs
   * asynchronously after the API call returns. The resolved future is the ID of the task, which can be used to
   * monitor its progress through the {@link #taskStatus(String)} API.
   *
   * @param idPrefix   Descriptive prefix to include at the start of task IDs
   * @param dataSource Datasource to kill
   * @param interval   Umbrella interval to be considered by the kill task. Note that unused segments falling in this
   *                   widened umbrella interval may have different {@code used_status_last_updated} time, so the kill task
   *                   should also filter by {@code maxUsedStatusLastUpdatedTime}
   * @param maxSegmentsToKill  The maximum number of segments to kill
   * @param maxUsedStatusLastUpdatedTime The maximum {@code used_status_last_updated} time. Any unused segment in {@code interval}
   *                                   with {@code used_status_last_updated} no later than this time will be included in the
   *                                   kill task. Segments without {@code used_status_last_updated} time (due to an upgrade
   *                                   from legacy Druid) will have {@code maxUsedStatusLastUpdatedTime} ignored
   *
   * @return future with task ID
   */
  default ListenableFuture<String> runKillTask(
      String idPrefix,
      String dataSource,
      Interval interval,
      @Nullable Integer maxSegmentsToKill,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    final String taskId = IdUtils.newTaskId(idPrefix, ClientKillUnusedSegmentsTaskQuery.TYPE, dataSource, interval);
    final ClientTaskQuery taskQuery = new ClientKillUnusedSegmentsTaskQuery(
        taskId,
        dataSource,
        interval,
        false,
        null,
        maxSegmentsToKill,
        maxUsedStatusLastUpdatedTime
    );
    return FutureUtils.transform(runTask(taskId, taskQuery), ignored -> taskId);
  }

  /**
   * Cancel a task.
   *
   * @param taskId task ID
   */
  ListenableFuture<Void> cancelTask(String taskId);

  /**
   * Return {@link TaskStatusPlus} for all tasks matching a set of optional search parameters.
   *
   * Complete tasks are returned in descending order by creation timestamp. Active tasks are returned in no
   * particular order.
   *
   * @param state             task state: may be "pending", "waiting", "running", or "complete"
   * @param dataSource        datasource
   * @param maxCompletedTasks maximum number of completed tasks to return. If zero, no complete tasks are returned.
   *                          If null, all complete tasks within {@code druid.indexer.storage.recentlyFinishedThreshold}
   *                          are returned. This parameter does not affect the number of active tasks returned.
   *
   * @return list of tasks that match the search parameters
   */
  ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
      @Nullable String state,
      @Nullable String dataSource,
      @Nullable Integer maxCompletedTasks
  );

  /**
   * Return {@link TaskStatus} for a set of task IDs.
   *
   * @param taskIds task IDs
   *
   * @return map of task ID to status for known tasks. Unknown tasks are not included in the returned map.
   */
  ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds);

  /**
   * Returns {@link TaskStatusResponse} for a particular task ID. This includes somewhat more information than
   * the {@link TaskStatus} returned by {@link #taskStatuses(Set)}.
   */
  ListenableFuture<TaskStatusResponse> taskStatus(String taskId);

  /**
   * Returns the report object for a task as a map. Certain task types offer live reports; for these task types,
   * this method may return a task report while the task is running. Certain task types only write reports upon
   * successful completion. Certain other task types do not write reports at all.
   *
   * Returns a {@link org.apache.druid.rpc.HttpResponseException} with code
   * {@link javax.ws.rs.core.Response.Status#NOT_FOUND} if there is no report available for some reason.
   */
  ListenableFuture<Map<String, Object>> taskReportAsMap(String taskId);

  /**
   * Returns the payload for a task as an instance of {@link ClientTaskQuery}. This method only works for tasks
   * that have a {@link ClientTaskQuery} model or are subclasses of {@link ClientTaskQuery}.
   */
  ListenableFuture<TaskPayloadResponse> taskPayload(String taskId);

  /**
   * Returns all current supervisor statuses.
   */
  ListenableFuture<CloseableIterator<SupervisorStatus>> supervisorStatuses();

  /**
   * Returns a list of intervals locked by higher priority conflicting lock types
   *
   * @param lockFilterPolicies List of all filters for different datasources
   * @return Map from datasource name to list of intervals locked by tasks that have a conflicting lock type with
   * priority greater than or equal to the {@code minTaskPriority} for that datasource.
   */
  ListenableFuture<Map<String, List<Interval>>> findLockedIntervals(
      List<LockFilterPolicy> lockFilterPolicies
  );

  /**
   * Deletes pending segment records from the metadata store for a particular datasource. Records with
   * {@code created_date} within the provided {@code interval} are deleted; other records are left alone.
   * Deletion is done synchronously with the API call. When the future resolves, the deletion is complete.
   *
   * @param dataSource datasource name
   * @param interval   created time interval
   *
   * @return number of pending segments deleted
   */
  ListenableFuture<Integer> killPendingSegments(String dataSource, Interval interval);

  /**
   * Returns information about workers.
   */
  ListenableFuture<List<IndexingWorkerInfo>> getWorkers();

  /**
   * Returns total worker capacity details.
   */
  ListenableFuture<IndexingTotalWorkerCapacityInfo> getTotalWorkerCapacity();

  /**
   * Returns a copy of this client with a different retry policy.
   */
  OverlordClient withRetryPolicy(ServiceRetryPolicy retryPolicy);
}
