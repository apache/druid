package com.metamx.druid.indexing.common.actions;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.indexing.common.TaskLock;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.coordinator.IndexerDBCoordinator;
import com.metamx.druid.indexing.coordinator.TaskLockbox;
import com.metamx.druid.indexing.coordinator.TaskQueue;
import com.metamx.emitter.service.ServiceEmitter;

import java.util.List;
import java.util.Set;

public class TaskActionToolbox
{
  private final TaskQueue taskQueue;
  private final TaskLockbox taskLockbox;
  private final IndexerDBCoordinator indexerDBCoordinator;
  private final ServiceEmitter emitter;

  @Inject
  public TaskActionToolbox(
      TaskQueue taskQueue,
      TaskLockbox taskLockbox,
      IndexerDBCoordinator indexerDBCoordinator,
      ServiceEmitter emitter
  )
  {
    this.taskQueue = taskQueue;
    this.taskLockbox = taskLockbox;
    this.indexerDBCoordinator = indexerDBCoordinator;
    this.emitter = emitter;
  }

  public TaskQueue getTaskQueue()
  {
    return taskQueue;
  }

  public TaskLockbox getTaskLockbox()
  {
    return taskLockbox;
  }

  public IndexerDBCoordinator getIndexerDBCoordinator()
  {
    return indexerDBCoordinator;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public boolean taskLockCoversSegments(
      final Task task,
      final Set<DataSegment> segments,
      final boolean allowOlderVersions
  )
  {
    // Verify that each of these segments falls under some lock

    // NOTE: It is possible for our lock to be revoked (if the task has failed and given up its locks) after we check
    // NOTE: it and before we perform the segment insert, but, that should be OK since the worst that happens is we
    // NOTE: insert some segments from the task but not others.

    final List<TaskLock> taskLocks = getTaskLockbox().findLocksForTask(task);
    for(final DataSegment segment : segments) {
      final boolean ok = Iterables.any(
          taskLocks, new Predicate<TaskLock>()
      {
        @Override
        public boolean apply(TaskLock taskLock)
        {
          final boolean versionOk = allowOlderVersions
                                    ? taskLock.getVersion().compareTo(segment.getVersion()) >= 0
                                    : taskLock.getVersion().equals(segment.getVersion());

          return versionOk
                 && taskLock.getDataSource().equals(segment.getDataSource())
                 && taskLock.getInterval().contains(segment.getInterval());
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
