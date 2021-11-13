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

package org.apache.druid.client.indexing;

import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IndexingServiceClient
{
  void killUnusedSegments(String idPrefix, String dataSource, Interval interval);

  int killPendingSegments(String dataSource, DateTime end);

  String compactSegments(
      String idPrefix,
      List<DataSegment> segments,
      int compactionTaskPriority,
      @Nullable ClientCompactionTaskQueryTuningConfig tuningConfig,
      @Nullable ClientCompactionTaskGranularitySpec granularitySpec,
      @Nullable ClientCompactionTaskDimensionsSpec dimensionsSpec,
      @Nullable Boolean dropExisting,
      @Nullable Map<String, Object> context
  );

  int getTotalWorkerCapacity();

  String runTask(String taskId, Object taskObject);

  String cancelTask(String taskId);

  /**
   * Gets all tasks that are waiting, pending, or running.
   */
  List<TaskStatusPlus> getActiveTasks();

  TaskStatusResponse getTaskStatus(String taskId);

  Map<String, TaskStatus> getTaskStatuses(Set<String> taskIds) throws InterruptedException;

  @Nullable
  TaskStatusPlus getLastCompleteTask();

  @Nullable
  TaskPayloadResponse getTaskPayload(String taskId);

  @Nullable
  Map<String, Object> getTaskReport(String taskId);

  /**
   * Gets a List of Intervals locked by higher priority tasks for each datasource.
   *
   * @param minTaskPriority Minimum task priority for each datasource. Only the
   *                        Intervals that are locked by Tasks higher than this
   *                        priority are returned. Tasks for datasources that
   *                        are not present in this Map are not returned.
   * @return Map from Datasource to List of Intervals locked by Tasks that have
   * priority strictly greater than the {@code minTaskPriority} for that datasource.
   */
  Map<String, List<Interval>> getLockedIntervals(Map<String, Integer> minTaskPriority);

  SamplerResponse sample(SamplerSpec samplerSpec);
}
