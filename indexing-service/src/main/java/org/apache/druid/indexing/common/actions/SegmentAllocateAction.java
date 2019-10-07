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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.LockRequestForNewSegment;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpecFactory;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Allocates a pending segment for a given timestamp. The preferredSegmentGranularity is used if there are no prior
 * segments for the given timestamp, or if the prior segments for the given timestamp are already at the
 * preferredSegmentGranularity. Otherwise, the prior segments will take precedence.
 * <p/>
 * This action implicitly acquires some task locks when it allocates segments. You do not have to acquire them
 * beforehand, although you *do* have to release them yourself. (Note that task locks are automatically released when
 * the task is finished.)
 * <p/>
 * If this action cannot acquire an appropriate task lock, or if it cannot expand an existing segment set, it returns
 * null.
 */
public class SegmentAllocateAction implements TaskAction<SegmentIdWithShardSpec>
{
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
  private final ShardSpecFactory shardSpecFactory;
  private final LockGranularity lockGranularity;

  @JsonCreator
  public SegmentAllocateAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("queryGranularity") Granularity queryGranularity,
      @JsonProperty("preferredSegmentGranularity") Granularity preferredSegmentGranularity,
      @JsonProperty("sequenceName") String sequenceName,
      @JsonProperty("previousSegmentId") String previousSegmentId,
      @JsonProperty("skipSegmentLineageCheck") boolean skipSegmentLineageCheck,
      @JsonProperty("shardSpecFactory") @Nullable ShardSpecFactory shardSpecFactory, // nullable for backward compatibility
      @JsonProperty("lockGranularity") @Nullable LockGranularity lockGranularity // nullable for backward compatibility
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
    this.shardSpecFactory = shardSpecFactory == null ? NumberedShardSpecFactory.instance() : shardSpecFactory;
    this.lockGranularity = lockGranularity == null ? LockGranularity.TIME_CHUNK : lockGranularity;
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

  @JsonProperty
  public ShardSpecFactory getShardSpecFactory()
  {
    return shardSpecFactory;
  }

  @JsonProperty
  public LockGranularity getLockGranularity()
  {
    return lockGranularity;
  }

  @Override
  public TypeReference<SegmentIdWithShardSpec> getReturnTypeReference()
  {
    return new TypeReference<SegmentIdWithShardSpec>()
    {
    };
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

      final Interval rowInterval = queryGranularity.bucket(timestamp);

      final Set<DataSegment> usedSegmentsForRow = new HashSet<>(
          msc.getUsedSegmentsForInterval(dataSource, rowInterval)
      );

      final SegmentIdWithShardSpec identifier = usedSegmentsForRow.isEmpty() ?
                                                tryAllocateFirstSegment(toolbox, task, rowInterval) :
                                                tryAllocateSubsequentSegment(
                                                    toolbox,
                                                    task,
                                                    rowInterval,
                                                    usedSegmentsForRow.iterator().next()
                                                );
      if (identifier != null) {
        return identifier;
      }

      // Could not allocate a pending segment. There's a chance that this is because someone else inserted a segment
      // overlapping with this row between when we called "mdc.getUsedSegmentsForInterval" and now. Check it again,
      // and if it's different, repeat.

      if (!ImmutableSet.copyOf(msc.getUsedSegmentsForInterval(dataSource, rowInterval)).equals(usedSegmentsForRow)) {
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
      log.error("The interval of existing segment[%s] doesn't contain rowInterval[%s]", usedSegment, rowInterval);
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
    // This action is always used by appending tasks, which cannot change the segment granularity of existing
    // dataSources. So, all lock requests should be segmentLock.
    final LockResult lockResult = toolbox.getTaskLockbox().tryLock(
        task,
        new LockRequestForNewSegment(
            lockGranularity,
            TaskLockType.EXCLUSIVE,
            task.getGroupId(),
            dataSource,
            tryInterval,
            shardSpecFactory,
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
           ", shardSpecFactory=" + shardSpecFactory +
           ", lockGranularity=" + lockGranularity +
           '}';
  }
}
