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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.indexing.ClientKillUnusedSegmentsTaskQuery;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.KillTaskReport;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.MarkSegmentsAsUnusedAction;
import org.apache.druid.indexing.common.actions.RetrieveUnusedSegmentsAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentNukeAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskLocks;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * The client representation of this task is {@link ClientKillUnusedSegmentsTaskQuery}.
 * JSON serialization fields of this class must correspond to those of {@link
 * ClientKillUnusedSegmentsTaskQuery}, except for {@link #id} and {@link #context} fields.
 * <p>
 * The field {@link #isMarkAsUnused()} is now deprecated.
 * </p>
 */
public class KillUnusedSegmentsTask extends AbstractFixedIntervalTask
{
  public static final String TYPE = "kill";
  private static final Logger LOG = new Logger(KillUnusedSegmentsTask.class);

  /**
   * Default nuke batch size. This is a small enough size that we still get value from batching, while
   * yielding as quickly as possible. In one real cluster environment backed with mysql, ~2000rows/sec,
   * with batch size of 100, means a batch should only less than a second for the task lock, and depending
   * on the segment store latency, unoptimised S3 cleanups typically take 5-10 seconds per 100. Over time
   * we expect the S3 cleanup to get quicker, so this should be < 1 second, which means we'll be yielding
   * the task lockbox every 1-2 seconds.
   */
  private static final int DEFAULT_SEGMENT_NUKE_BATCH_SIZE = 100;

  @Deprecated
  private final boolean markAsUnused;
  /**
   * Split processing to try and keep each nuke operation relatively short, in the case that either
   * the database or the storage layer is particularly slow.
   */
  private final int batchSize;

  /**
   * Maximum number of segments that can be killed.
   */
  @Nullable private final Integer limit;

  /**
   * The maximum used status last updated time. Any segments with
   * {@code used_status_last_updated} no later than this time will be included in the kill task.
   */
  @Nullable private final DateTime maxUsedStatusLastUpdatedTime;

  @JsonCreator
  public KillUnusedSegmentsTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("markAsUnused") @Deprecated Boolean markAsUnused,
      @JsonProperty("batchSize") Integer batchSize,
      @JsonProperty("limit") @Nullable Integer limit,
      @JsonProperty("maxUsedStatusLastUpdatedTime") @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    super(
        getOrMakeId(id, "kill", dataSource, interval),
        dataSource,
        interval,
        context
    );
    this.markAsUnused = markAsUnused != null && markAsUnused;
    this.batchSize = (batchSize != null) ? batchSize : DEFAULT_SEGMENT_NUKE_BATCH_SIZE;
    if (this.batchSize <= 0) {
      throw InvalidInput.exception("batchSize[%d] must be a positive integer.", batchSize);
    }
    if (limit != null && limit <= 0) {
      throw InvalidInput.exception("limit[%d] must be a positive integer.", limit);
    }
    if (limit != null && Boolean.TRUE.equals(markAsUnused)) {
      throw InvalidInput.exception("limit[%d] cannot be provided when markAsUnused is enabled.", limit);
    }
    this.limit = limit;
    this.maxUsedStatusLastUpdatedTime = maxUsedStatusLastUpdatedTime;
  }

  /**
   * This field has been deprecated as "kill" tasks should not be responsible for
   * marking segments as unused. Instead, users should call the Coordinator API
   * {@code /{dataSourceName}/markUnused} to explicitly mark segments as unused.
   * Segments may also be marked unused by the Coordinator if they become overshadowed
   * or have a {@code DropRule} applied to them.
   */
  @Deprecated
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isMarkAsUnused()
  {
    return markAsUnused;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getBatchSize()
  {
    return batchSize;
  }

  @Nullable
  @JsonProperty
  public Integer getLimit()
  {
    return limit;
  }

  @Nullable
  @JsonProperty
  public DateTime getMaxUsedStatusLastUpdatedTime()
  {
    return maxUsedStatusLastUpdatedTime;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    return ImmutableSet.of();
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    // Track stats for reporting
    int numSegmentsKilled = 0;
    int numBatchesProcessed = 0;
    final int numSegmentsMarkedAsUnused;
    if (markAsUnused) {
      numSegmentsMarkedAsUnused = toolbox.getTaskActionClient().submit(
          new MarkSegmentsAsUnusedAction(getDataSource(), getInterval())
      );
      LOG.info("Marked [%d] segments of datasource[%s] in interval[%s] as unused.",
               numSegmentsMarkedAsUnused, getDataSource(), getInterval());
    } else {
      numSegmentsMarkedAsUnused = 0;
    }

    // List unused segments
    int nextBatchSize = computeNextBatchSize(numSegmentsKilled);
    @Nullable Integer numTotalBatches = getNumTotalBatches();
    List<DataSegment> unusedSegments;
    LOG.info(
        "Starting kill for datasource[%s] in interval[%s] with batchSize[%d], up to limit[%d] segments "
        + "before maxUsedStatusLastUpdatedTime[%s] will be deleted%s",
        getDataSource(),
        getInterval(),
        batchSize,
        limit,
        maxUsedStatusLastUpdatedTime,
        numTotalBatches != null ? StringUtils.format(" in [%d] batches.", numTotalBatches) : "."
    );

    RetrieveUsedSegmentsAction retrieveUsedSegmentsAction = new RetrieveUsedSegmentsAction(
            getDataSource(),
            null,
            ImmutableList.of(getInterval()),
            Segments.INCLUDING_OVERSHADOWED
    );
    // Fetch the load specs of all segments overlapping with the unused segment intervals
    final Set<Map<String, Object>> usedSegmentLoadSpecs =
            new HashSet<>(toolbox.getTaskActionClient().submit(retrieveUsedSegmentsAction)
                    .stream()
                    .map(DataSegment::getLoadSpec)
                    .collect(Collectors.toSet())
            );

    do {
      if (nextBatchSize <= 0) {
        break;
      }

      unusedSegments = toolbox
              .getTaskActionClient()
              .submit(
                  new RetrieveUnusedSegmentsAction(getDataSource(), getInterval(), nextBatchSize, maxUsedStatusLastUpdatedTime
              ));

      // Fetch locks each time as a revokal could have occurred in between batches
      final NavigableMap<DateTime, List<TaskLock>> taskLockMap
              = getNonRevokedTaskLockMap(toolbox.getTaskActionClient());

      if (!TaskLocks.isLockCoversSegments(taskLockMap, unusedSegments)) {
        throw new ISE(
                "Locks[%s] for task[%s] can't cover segments[%s]",
                taskLockMap.values().stream().flatMap(List::stream).collect(Collectors.toList()),
                getId(),
                unusedSegments
        );
      }

      // Kill segments
      // Order is important here: we want the nuke action to clean up the metadata records _before_ the
      // segments are removed from storage, this helps maintain that we will always have a storage segment if
      // the metadata segment is present. If the segment nuke throws an exception, then the segment cleanup is
      // abandoned.

      toolbox.getTaskActionClient().submit(new SegmentNukeAction(new HashSet<>(unusedSegments)));


      // Kill segments from the deep storage only if their load specs are not being used by any used segments
      final List<DataSegment> segmentsToBeKilled = unusedSegments
          .stream()
          .filter(unusedSegment -> unusedSegment.getLoadSpec() == null
                                   || !usedSegmentLoadSpecs.contains(unusedSegment.getLoadSpec()))
          .collect(Collectors.toList());

      toolbox.getDataSegmentKiller().kill(segmentsToBeKilled);
      numBatchesProcessed++;
      numSegmentsKilled += unusedSegments.size();

      LOG.info("Processed [%d] batches for kill task[%s].", numBatchesProcessed, getId());

      nextBatchSize = computeNextBatchSize(numSegmentsKilled);
    } while (unusedSegments.size() != 0 && (null == numTotalBatches || numBatchesProcessed < numTotalBatches));

    final String taskId = getId();
    LOG.info(
        "Finished kill task[%s] for dataSource[%s] and interval[%s]."
        + " Deleted total [%d] unused segments in [%d] batches.",
        taskId, getDataSource(), getInterval(), numSegmentsKilled, numBatchesProcessed
    );

    final KillTaskReport.Stats stats =
        new KillTaskReport.Stats(numSegmentsKilled, numBatchesProcessed, numSegmentsMarkedAsUnused);
    toolbox.getTaskReportFileWriter().write(
        taskId,
        TaskReport.buildTaskReports(new KillTaskReport(taskId, stats))
    );

    return TaskStatus.success(taskId);
  }

  @JsonIgnore
  @VisibleForTesting
  @Nullable
  Integer getNumTotalBatches()
  {
    return null != limit ? (int) Math.ceil((double) limit / batchSize) : null;
  }

  @JsonIgnore
  @VisibleForTesting
  int computeNextBatchSize(int numSegmentsKilled)
  {
    return null != limit ? Math.min(limit - numSegmentsKilled, batchSize) : batchSize;
  }

  private NavigableMap<DateTime, List<TaskLock>> getNonRevokedTaskLockMap(TaskActionClient client) throws IOException
  {
    final NavigableMap<DateTime, List<TaskLock>> taskLockMap = new TreeMap<>();
    getTaskLocks(client).forEach(
        taskLock -> {
          if (!taskLock.isRevoked()) {
            taskLockMap.computeIfAbsent(taskLock.getInterval().getStart(), k -> new ArrayList<>()).add(taskLock);
          }
        }
    );
    return taskLockMap;
  }
}
