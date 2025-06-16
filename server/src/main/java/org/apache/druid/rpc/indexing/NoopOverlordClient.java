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
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.IndexingWorkerInfo;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.server.http.SegmentsToUpdateFilter;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link OverlordClient} that throws
 * {@link UnsupportedOperationException} for every method.
 */
public class NoopOverlordClient implements OverlordClient
{
  @Override
  public ListenableFuture<URI> findCurrentLeader()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Void> runTask(String taskId, Object taskObject)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Void> cancelTask(String taskId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
      @Nullable String state,
      @Nullable String dataSource,
      @Nullable Integer maxCompletedTasks
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<TaskStatusResponse> taskStatus(String taskId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<TaskPayloadResponse> taskPayload(String taskId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<TaskReport.ReportMap> taskReportAsMap(String taskId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Map<String, String>> postSupervisor(SupervisorSpec supervisor)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<CloseableIterator<SupervisorStatus>> supervisorStatuses()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Map<String, List<Interval>>> findLockedIntervals(
      List<LockFilterPolicy> lockFilterPolicies
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Integer> killPendingSegments(String dataSource, Interval interval)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<List<IndexingWorkerInfo>> getWorkers()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<IndexingTotalWorkerCapacityInfo> getTotalWorkerCapacity()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<SegmentUpdateResponse> markNonOvershadowedSegmentsAsUsed(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<SegmentUpdateResponse> markNonOvershadowedSegmentsAsUsed(
      String dataSource,
      SegmentsToUpdateFilter filter
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<SegmentUpdateResponse> markSegmentAsUsed(SegmentId segmentId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<SegmentUpdateResponse> markSegmentsAsUnused(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<SegmentUpdateResponse> markSegmentsAsUnused(String dataSource, SegmentsToUpdateFilter filter)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<SegmentUpdateResponse> markSegmentAsUnused(SegmentId segmentId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<Boolean> isCompactionSupervisorEnabled()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public OverlordClient withRetryPolicy(ServiceRetryPolicy retryPolicy)
  {
    // Ignore retryPolicy for the test client.
    return this;
  }
}
