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
  void killSegments(String dataSource, Interval interval);

  int killPendingSegments(String dataSource, DateTime end);

  String compactSegments(
      List<DataSegment> segments,
      int compactionTaskPriority,
      @Nullable ClientCompactQueryTuningConfig tuningConfig,
      @Nullable Map<String, Object> context
  );

  int getTotalWorkerCapacity();

  String runTask(Object taskObject);

  String killTask(String taskId);

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
}
