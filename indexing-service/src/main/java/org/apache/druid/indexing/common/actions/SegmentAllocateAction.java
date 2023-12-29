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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.LockRequestForNewSegment;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.PartialShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Allocates a pending segment for a given timestamp.
 * If a visible chunk of used segments contains the interval with the query granularity containing the timestamp,
 * the pending segment is allocated with its interval.
 * Else, if the interval with the preferred segment granularity containing the timestamp has no overlap
 * with the existing used segments, the preferred segment granularity is used.
 * Else, find the coarsest segment granularity, containing the interval with the query granularity for the timestamp,
 * that does not overlap with the existing used segments. This granularity is used for allocation if it exists.
 * <p/>
 * This action implicitly acquires some task locks when it allocates segments. You do not have to acquire them
 * beforehand, although you *do* have to release them yourself. (Note that task locks are automatically released when
 * the task is finished.)
 * <p/>
 * If this action cannot acquire an appropriate task lock, or if it cannot expand an existing segment set, it returns
 * null.
 * </p>
 * Do NOT allocate WEEK granularity segments unless the preferred segment granularity is WEEK.
 */
public class SegmentAllocateAction implements TaskAction<SegmentIdWithShardSpec>
{
  public static final String TYPE = "segmentAllocate";

  private static final Logger log = new Logger(SegmentAllocateAction.class);

  // Prevent spinning forever in situations where the segment list just won't stop changing.
  private static final int MAX_ATTEMPTS = 90;

  private final String dataSource;
  private final DateTime timestamp;
  private final Granularity queryGranularity;
  private final Granularity preferredSegmentGranularity;
  private final String sequenceName;
  private final String previousSegmentId;
  private final boolean skipSegmentLineageCheck;
  private final PartialShardSpec partialShardSpec;
  private final LockGranularity lockGranularity;
  private final TaskLockType taskLockType;

  @JsonCreator
  public SegmentAllocateAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("queryGranularity") Granularity queryGranularity,
      @JsonProperty("preferredSegmentGranularity") Granularity preferredSegmentGranularity,
      @JsonProperty("sequenceName") String sequenceName,
      @JsonProperty("previousSegmentId") String previousSegmentId,
      @JsonProperty("skipSegmentLineageCheck") boolean skipSegmentLineageCheck,
      // nullable for backward compatibility
      @JsonProperty("shardSpecFactory") @Nullable PartialShardSpec partialShardSpec,
      @JsonProperty("lockGranularity") @Nullable LockGranularity lockGranularity,
      @JsonProperty("taskLockType") @Nullable TaskLockType taskLockType
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.timestamp = Preconditions.checkNotNull(timestamp, "timestamp");
    this.queryGranularity = Preconditions.checkNotNull(queryGranularity, "queryGranularity");
    this.preferredSegmentGranularity = Preconditions.checkNotNull(
        preferredSegmentGranularity,
        "preferredSegmentGranularity"
    );
    this.sequenceName = Preconditions.checkNotNull(sequenceName, "sequenceName");
    this.previousSegmentId = previousSegmentId;
    this.skipSegmentLineageCheck = skipSegmentLineageCheck;
    this.partialShardSpec = partialShardSpec == null ? NumberedPartialShardSpec.instance() : partialShardSpec;
    this.lockGranularity = lockGranularity == null ? LockGranularity.TIME_CHUNK : lockGranularity;
    this.taskLockType = taskLockType == null ? TaskLockType.EXCLUSIVE : taskLockType;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty
  public Granularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty
  public Granularity getPreferredSegmentGranularity()
  {
    return preferredSegmentGranularity;
  }

  @JsonProperty
  public String getSequenceName()
  {
    return sequenceName;
  }

  @JsonProperty
  public String getPreviousSegmentId()
  {
    return previousSegmentId;
  }

  @JsonProperty
  public boolean isSkipSegmentLineageCheck()
  {
    return skipSegmentLineageCheck;
  }

  @JsonProperty("shardSpecFactory")
  public PartialShardSpec getPartialShardSpec()
  {
    return partialShardSpec;
  }

  @JsonProperty
  public LockGranularity getLockGranularity()
  {
    return lockGranularity;
  }

  @JsonProperty
  public TaskLockType getTaskLockType()
  {
    return taskLockType;
  }

  @Override
  public TypeReference<SegmentIdWithShardSpec> getReturnTypeReference()
  {
    return new TypeReference<SegmentIdWithShardSpec>()
    {
    };
  }

  @Override
  public boolean canPerformAsync(Task task, TaskActionToolbox toolbox)
  {
    return toolbox.canBatchSegmentAllocation();
  }

  @Override
  public Future<SegmentIdWithShardSpec> performAsync(Task task, TaskActionToolbox toolbox)
  {
    if (!toolbox.canBatchSegmentAllocation()) {
      throw new ISE("Batched segment allocation is disabled");
    }
    return toolbox.getSegmentAllocationQueue().add(
        new SegmentAllocateRequest(task, this, MAX_ATTEMPTS)
    );
  }

  @Override
  public SegmentIdWithShardSpec perform(
      final Task task,
      final TaskActionToolbox toolbox
  )
  {
    int attempt = 0;
    while (true) {
      attempt++;

      if (!task.getDataSource().equals(dataSource)) {
        throw new IAE("Task dataSource must match action dataSource, [%s] != [%s].", task.getDataSource(), dataSource);
      }

      final IndexerMetadataStorageCoordinator msc = toolbox.getIndexerMetadataStorageCoordinator();

      // 1) if something overlaps our timestamp, use that
      // 2) otherwise try preferredSegmentGranularity & going progressively smaller

      final Interval rowInterval = queryGranularity.bucket(timestamp).withChronology(ISOChronology.getInstanceUTC());

      final Set<DataSegment> usedSegmentsForRow =
          new HashSet<>(msc.retrieveUsedSegmentsForInterval(dataSource, rowInterval, Segments.ONLY_VISIBLE));

      final SegmentIdWithShardSpec identifier;
      if (usedSegmentsForRow.isEmpty()) {
        identifier = tryAllocateFirstSegment(toolbox, task, rowInterval);
      } else {
        identifier = tryAllocateSubsequentSegment(toolbox, task, rowInterval, usedSegmentsForRow.iterator().next());
      }
      if (identifier != null) {
        return identifier;
      }

      // Could not allocate a pending segment. There's a chance that this is because someone else inserted a segment
      // overlapping with this row between when we called "msc.retrieveUsedSegmentsForInterval" and now. Check it again,
      // and if it's different, repeat.

      Set<DataSegment> newUsedSegmentsForRow =
          new HashSet<>(msc.retrieveUsedSegmentsForInterval(dataSource, rowInterval, Segments.ONLY_VISIBLE));
      if (!newUsedSegmentsForRow.equals(usedSegmentsForRow)) {
        if (attempt < MAX_ATTEMPTS) {
          final long shortRandomSleep = 50 + (long) (ThreadLocalRandom.current().nextDouble() * 450);
          log.debug(
              "Used segment set changed for rowInterval[%s]. Retrying segment allocation in %,dms (attempt = %,d).",
              rowInterval,
              shortRandomSleep,
              attempt
          );
          try {
            Thread.sleep(shortRandomSleep);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        } else {
          log.error(
              "Used segment set changed for rowInterval[%s]. Not trying again (attempt = %,d).",
              rowInterval,
              attempt
          );
          return null;
        }
      } else {
        return null;
      }
    }
  }

  private SegmentIdWithShardSpec tryAllocateFirstSegment(TaskActionToolbox toolbox, Task task, Interval rowInterval)
  {
    // No existing segments for this row, but there might still be nearby ones that conflict with our preferred
    // segment granularity. Try that first, and then progressively smaller ones if it fails.
    final List<Interval> tryIntervals = Granularity.granularitiesFinerThan(preferredSegmentGranularity)
                                                   .stream()
                                                   .map(granularity -> granularity.bucket(timestamp))
                                                   .collect(Collectors.toList());
    for (Interval tryInterval : tryIntervals) {
      if (tryInterval.contains(rowInterval)) {
        final SegmentIdWithShardSpec identifier = tryAllocate(toolbox, task, tryInterval, rowInterval, false);
        if (identifier != null) {
          return identifier;
        }
      }
    }
    return null;
  }

  private SegmentIdWithShardSpec tryAllocateSubsequentSegment(
      TaskActionToolbox toolbox,
      Task task,
      Interval rowInterval,
      DataSegment usedSegment
  )
  {
    // Existing segment(s) exist for this row; use the interval of the first one.
    if (!usedSegment.getInterval().contains(rowInterval)) {
      log.error(
          "The interval of existing segment[%s] doesn't contain rowInterval[%s]",
          usedSegment.getId(),
          rowInterval
      );
      return null;
    } else {
      // If segment allocation failed here, it is highly likely an unrecoverable error. We log here for easier
      // debugging.
      return tryAllocate(toolbox, task, usedSegment.getInterval(), rowInterval, true);
    }
  }

  private SegmentIdWithShardSpec tryAllocate(
      TaskActionToolbox toolbox,
      Task task,
      Interval tryInterval,
      Interval rowInterval,
      boolean logOnFail
  )
  {
    // This action is always used by appending tasks, so if it is a time_chunk lock then we allow it to be
    // shared with other appending tasks as well
    final LockResult lockResult = toolbox.getTaskLockbox().tryLock(
        task,
        new LockRequestForNewSegment(
            lockGranularity,
            taskLockType,
            task.getGroupId(),
            dataSource,
            tryInterval,
            partialShardSpec,
            task.getPriority(),
            sequenceName,
            previousSegmentId,
            skipSegmentLineageCheck
        )
    );

    if (lockResult.isRevoked()) {
      // We had acquired a lock but it was preempted by other locks
      throw new ISE("The lock for interval[%s] is preempted and no longer valid", tryInterval);
    }

    if (lockResult.isOk()) {
      final SegmentIdWithShardSpec identifier = lockResult.getNewSegmentId();
      if (identifier != null) {
        return identifier;
      } else {
        final String msg = StringUtils.format(
            "Could not allocate pending segment for rowInterval[%s], segmentInterval[%s].",
            rowInterval,
            tryInterval
        );
        if (logOnFail) {
          log.error(msg);
        } else {
          log.debug(msg);
        }
        return null;
      }
    } else {
      final String msg = StringUtils.format(
          "Could not acquire lock for rowInterval[%s], segmentInterval[%s].",
          rowInterval,
          tryInterval
      );
      if (logOnFail) {
        log.error(msg);
      } else {
        log.debug(msg);
      }
      return null;
    }
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "SegmentAllocateAction{" +
           "dataSource='" + dataSource + '\'' +
           ", timestamp=" + timestamp +
           ", queryGranularity=" + queryGranularity +
           ", preferredSegmentGranularity=" + preferredSegmentGranularity +
           ", sequenceName='" + sequenceName + '\'' +
           ", previousSegmentId='" + previousSegmentId + '\'' +
           ", skipSegmentLineageCheck=" + skipSegmentLineageCheck +
           ", partialShardSpec=" + partialShardSpec +
           ", lockGranularity=" + lockGranularity +
           '}';
  }
}
