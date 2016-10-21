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

package io.druid.indexing.common.actions;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.java.util.common.ISE;
import io.druid.timeline.DataSegment;

import java.util.List;
import java.util.Set;

public class TaskActionToolbox
{
  private final TaskLockbox taskLockbox;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final ServiceEmitter emitter;

  @Inject
  public TaskActionToolbox(
      TaskLockbox taskLockbox,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      ServiceEmitter emitter
  )
  {
    this.taskLockbox = taskLockbox;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.emitter = emitter;
  }

  public TaskLockbox getTaskLockbox()
  {
    return taskLockbox;
  }

  public IndexerMetadataStorageCoordinator getIndexerMetadataStorageCoordinator()
  {
    return indexerMetadataStorageCoordinator;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public void verifyTaskLocks(
      final Task task,
      final Set<DataSegment> segments
  )
  {
    if (!taskLockCoversSegments(task, segments)) {
      throw new ISE("Segments not covered by locks for task: %s", task.getId());
    }
  }

  public boolean taskLockCoversSegments(
      final Task task,
      final Set<DataSegment> segments
  )
  {
    // Verify that each of these segments falls under some lock

    // NOTE: It is possible for our lock to be revoked (if the task has failed and given up its locks) after we check
    // NOTE: it and before we perform the segment insert, but, that should be OK since the worst that happens is we
    // NOTE: insert some segments from the task but not others.

    final List<TaskLock> taskLocks = getTaskLockbox().findLocksForTask(task);
    for (final DataSegment segment : segments) {
      final boolean ok = Iterables.any(
          taskLocks, new Predicate<TaskLock>()
          {
            @Override
            public boolean apply(TaskLock taskLock)
            {
              return taskLock.getDataSource().equals(segment.getDataSource())
                     && taskLock.getInterval().contains(segment.getInterval())
                     && taskLock.getVersion().compareTo(segment.getVersion()) >= 0;
            }
          }
      );

      if (!ok) {
        return false;
      }
    }

    return true;
  }
}
