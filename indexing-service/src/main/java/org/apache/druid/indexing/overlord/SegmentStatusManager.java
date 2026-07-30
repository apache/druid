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
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.task.AbstractFixedIntervalTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskMetrics;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Performs updates on segments to change their "used" status in the metadata store.
 * <p>
 * Currently, this class exposes methods that are used to acquire an EXCLUSIVE
 * lock while marking segments in an interval as "used". A similar restriction
 * may be imposed on marking (non-overshadowed) segments as unused in the future.
 *
 * @see #markAsUsedWithExclusiveLock(String, Interval, UpdateOperation)
 * Reasons for using EXCLUSIVE locks
 */
public class SegmentStatusManager
{
  private static final Logger log = new Logger(SegmentStatusManager.class);
  private static final String TASK_TYPE_MARK_USED = "markSegmentAsUsed";

  private final ServiceEmitter emitter;
  private final GlobalTaskLockbox taskLockbox;
  private final TaskActionClientFactory taskActionClientFactory;
  private final IndexerMetadataStorageCoordinator storageCoordinator;

  @Inject
  public SegmentStatusManager(
      GlobalTaskLockbox taskLockbox,
      IndexerMetadataStorageCoordinator storageCoordinator,
      TaskActionClientFactory taskActionClientFactory,
      ServiceEmitter emitter
  )
  {
    this.emitter = emitter;
    this.taskLockbox = taskLockbox;
    this.storageCoordinator = storageCoordinator;
    this.taskActionClientFactory = taskActionClientFactory;
  }

  /**
   * Same as {@link IndexerMetadataStorageCoordinator#markAllNonOvershadowedSegmentsAsUsed(String)}
   * but with an EXCLUSIVE lock.
   */
  public int markAllNonOvershadowedSegmentsAsUsed(String dataSource)
  {
    return markAsUsedWithExclusiveLock(
        dataSource,
        Intervals.ETERNITY,
        () -> storageCoordinator.markAllNonOvershadowedSegmentsAsUsed(dataSource)
    );
  }

  /**
   * Same as {@link IndexerMetadataStorageCoordinator#markNonOvershadowedSegmentsAsUsed(String, Interval, List)}
   * but with an EXCLUSIVE lock.
   */
  public int markNonOvershadowedSegmentsAsUsed(
      String dataSource,
      Interval interval,
      @Nullable List<String> versions
  )
  {
    return markAsUsedWithExclusiveLock(
        dataSource,
        interval,
        () -> storageCoordinator.markNonOvershadowedSegmentsAsUsed(dataSource, interval, versions)
    );
  }

  /**
   * Same as {@link IndexerMetadataStorageCoordinator#markNonOvershadowedSegmentsAsUsed(String, Set)}
   * but with an EXCLUSIVE lock.
   */
  public int markNonOvershadowedSegmentsAsUsed(String dataSource, Set<SegmentId> segmentIds)
  {
    return markAsUsedWithExclusiveLock(
        dataSource,
        JodaUtils.umbrellaInterval(segmentIds.stream().map(SegmentId::getInterval).toList()),
        () -> storageCoordinator.markNonOvershadowedSegmentsAsUsed(dataSource, segmentIds)
    );
  }

  /**
   * Same as {@link IndexerMetadataStorageCoordinator#markSegmentAsUsed(SegmentId)}
   * but with an EXCLUSIVE lock.
   */
  public int markSegmentAsUsed(SegmentId segmentId)
  {
    return markAsUsedWithExclusiveLock(
        segmentId.getDataSource(),
        segmentId.getInterval(),
        () -> storageCoordinator.markSegmentAsUsed(segmentId) ? 1 : 0
    );
  }

  /**
   * Mark segments for a datasource-interval as used while holding an EXCLUSIVE
   * lock. Most APIs mark non-overshadowed segments as used. So, as soon as they
   * are updated, they would become visible to other metadata operations. This
   * may cause concurrent append jobs to see an inconsistent view of existing
   * segments during their lifecycle. It may also cause a concurrent kill task
   * to accidentally delete files of a segment that has just been marked as used
   * and then upgraded.
   *
   * @return Number of segments updated.
   */
  private int markAsUsedWithExclusiveLock(String dataSource, Interval interval, UpdateOperation operation)
  {
    final Stopwatch taskRunTime = Stopwatch.createStarted();

    final String taskId = IdUtils.newTaskId(TASK_TYPE_MARK_USED, dataSource, interval);
    final ExclusiveIntervalDummyTask dummyTask
        = new ExclusiveIntervalDummyTask(taskId, dataSource, interval);

    final TaskActionClient taskActionClient = taskActionClientFactory.create(dummyTask);

    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
    metricBuilder.setDimension(DruidMetrics.INTERVAL, interval);
    metricBuilder.setDimension(DruidMetrics.DATASOURCE, dataSource);

    try {
      // Acquire lock on the interval before performing the update operation
      taskLockbox.add(dummyTask);
      final boolean isReady = dummyTask.isReady(taskActionClient);
      if (isReady) {
        return operation.perform();
      } else {
        final String message =
            "Could not acquire lock over interval[%s] of datasource[%s] since other tasks are in progress."
            + " Retry after the tasks have completed.";
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.CONFLICT)
                            .build(message, interval, dataSource);
      }
    }
    catch (DruidException e) {
      throw e;
    }
    catch (Exception e) {
      throw InternalServerError.exception(
          e,
          "Could not mark segments in interval[%s] of datasource[%s] as used.",
          interval, dataSource
      );
    }
    finally {
      cleanupLocksSilently(dummyTask);
      emitter.emit(metricBuilder.setMetric(TaskMetrics.RUN_DURATION, taskRunTime.millisElapsed()));
    }
  }

  private void cleanupLocksSilently(Task task)
  {
    try {
      taskLockbox.remove(task);
    }
    catch (Throwable t) {
      log.error(t, "Error while cleaning up locks for embedded task[%s].", task.getId());
    }
  }

  /**
   * Dummy task used to acquire an {@link TaskLockType#EXCLUSIVE} lock while marking
   * segments as used. The lock is acquired to ensure that the operation is mutually
   * exclusive with any other task on an overlapping interval and prevent data
   * losses (due to kill tasks removing the files of an upgraded segment) or
   * data inconsistencies (due to append tasks seeing multiple versions of used
   * segments in an interval).
   */
  private static class ExclusiveIntervalDummyTask extends AbstractFixedIntervalTask
  {
    public ExclusiveIntervalDummyTask(String id, String dataSource, Interval interval)
    {
      super(id, dataSource, interval, Map.of());
    }

    @Override
    public TaskStatus runTask(TaskToolbox taskToolbox)
    {
      // Do nothing here, this task is never run
      return TaskStatus.success(getId());
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient) throws Exception
    {
      final TaskLock lock = taskActionClient.submit(
          new TimeChunkLockTryAcquireAction(TaskLockType.EXCLUSIVE, getInterval())
      );
      if (lock == null) {
        return false;
      }
      lock.assertNotRevoked();
      return true;
    }

    @Override
    public String getType()
    {
      return TASK_TYPE_MARK_USED;
    }
  }

  @FunctionalInterface
  private interface UpdateOperation
  {
    int perform();
  }
}
