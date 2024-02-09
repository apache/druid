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

package org.apache.druid.indexing.overlord;

import com.google.inject.Inject;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.Interval;

import java.util.Comparator;
import java.util.Optional;

public class IndexerMetadataStorageAdapter
{
  private final TaskStorageQueryAdapter taskStorageQueryAdapter;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;

  @Inject
  public IndexerMetadataStorageAdapter(
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator
  )
  {
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
  }

  public int deletePendingSegments(String dataSource, Interval deleteInterval)
  {
    // Find the earliest active task created for the specified datasource; if one exists,
    // check if its interval overlaps with the delete interval.
    final Optional<TaskInfo<Task, TaskStatus>> earliestActiveTaskOptional = taskStorageQueryAdapter
        .getActiveTaskInfo(dataSource)
        .stream()
        .min(Comparator.comparing(TaskInfo::getCreatedTime));

    if (earliestActiveTaskOptional.isPresent()) {
      final TaskInfo<Task, TaskStatus> earliestActiveTask = earliestActiveTaskOptional.get();
      final Interval activeTaskInterval = new Interval(
          earliestActiveTask.getCreatedTime(),
          DateTimes.MAX
      );

      if (deleteInterval.overlaps(activeTaskInterval)) {
        throw InvalidInput.exception(
            "Cannot delete pendingSegments for datasource[%s] as there is at least one active task[%s] created at[%s] "
            + "that overlaps with the delete interval[%s]. Please retry when there are no active tasks.",
            dataSource,
            earliestActiveTask.getId(),
            activeTaskInterval.getStart(),
            deleteInterval
        );
      }
    }

    return indexerMetadataStorageCoordinator.deletePendingSegmentsCreatedInInterval(dataSource, deleteInterval);
  }
}
