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

import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.SegmentLockReleaseAction;
import org.apache.druid.indexing.common.actions.SegmentLockTryAcquireAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class provides 3 functionalities.
 * - {@link #verifyAndLockExistingSegments} is to verify the granularity of existing segments and lock them.
 *   This method must be called before the task starts indexing.
 * - Tells the task what {@link LockGranularity} it should use. Note that the LockGranularity is determined in
 *   {@link AbstractBatchIndexTask#determineLockGranularityandTryLock}.
 * - Provides some util methods for {@link LockGranularity#SEGMENT}. Also caches the information of locked segments when
 *   - the SEGMENt lock granularity is used.
 */
public class TaskLockHelper
{
  private final Map<Interval, OverwritingRootGenerationPartitions> overwritingRootGenPartitions = new HashMap<>();
  private final Set<DataSegment> lockedExistingSegments = new HashSet<>();
  private final boolean useSegmentLock;

  @Nullable
  private Granularity knownSegmentGranularity;

  public static class OverwritingRootGenerationPartitions
  {
    private final int startRootPartitionId;
    private final int endRootPartitionId;
    private final short maxMinorVersion;

    private OverwritingRootGenerationPartitions(int startRootPartitionId, int endRootPartitionId, short maxMinorVersion)
    {
      this.startRootPartitionId = startRootPartitionId;
      this.endRootPartitionId = endRootPartitionId;
      this.maxMinorVersion = maxMinorVersion;
    }

    public int getStartRootPartitionId()
    {
      return startRootPartitionId;
    }

    public int getEndRootPartitionId()
    {
      return endRootPartitionId;
    }

    public short getMinorVersionForNewSegments()
    {
      return (short) (maxMinorVersion + 1);
    }
  }

  public TaskLockHelper(boolean useSegmentLock)
  {
    this.useSegmentLock = useSegmentLock;
  }

  public boolean isUseSegmentLock()
  {
    return useSegmentLock;
  }

  public LockGranularity getLockGranularityToUse()
  {
    return useSegmentLock ? LockGranularity.SEGMENT : LockGranularity.TIME_CHUNK;
  }

  public boolean hasLockedExistingSegments()
  {
    return !lockedExistingSegments.isEmpty();
  }

  public boolean hasOverwritingRootGenerationPartition(Interval interval)
  {
    return overwritingRootGenPartitions.containsKey(interval);
  }

  public Set<DataSegment> getLockedExistingSegments()
  {
    return Collections.unmodifiableSet(lockedExistingSegments);
  }

  public OverwritingRootGenerationPartitions getOverwritingRootGenerationPartition(Interval interval)
  {
    return overwritingRootGenPartitions.get(interval);
  }

  boolean verifyAndLockExistingSegments(TaskActionClient actionClient, List<DataSegment> segments)
      throws IOException
  {
    final List<DataSegment> segmentsToLock = segments.stream()
                                                     .filter(segment -> !lockedExistingSegments.contains(segment))
                                                     .collect(Collectors.toList());
    if (segmentsToLock.isEmpty()) {
      return true;
    }

    verifySegmentGranularity(segmentsToLock);
    return tryLockSegments(actionClient, segmentsToLock);
  }

  /**
   * Check if segmentGranularity has changed.
   */
  private void verifySegmentGranularity(List<DataSegment> segments)
  {
    final Granularity granularityFromSegments = AbstractBatchIndexTask.findGranularityFromSegments(segments);
    if (granularityFromSegments != null) {
      if (knownSegmentGranularity == null) {
        knownSegmentGranularity = granularityFromSegments;
      } else {
        if (!knownSegmentGranularity.equals(granularityFromSegments)) {
          throw new ISE(
              "Found a different granularity from knownSegmentGranularity[%s] in segments[%s]",
              knownSegmentGranularity,
              segments
          );
        }
        final List<DataSegment> nonAlignedSegments = segments
            .stream()
            .filter(segment -> !knownSegmentGranularity.isAligned(segment.getInterval()))
            .collect(Collectors.toList());

        if (!nonAlignedSegments.isEmpty()) {
          throw new ISE(
              "Non-aligned segments %s for granularity[%s]",
              SegmentUtils.commaSeparatedIdentifiers(nonAlignedSegments),
              knownSegmentGranularity
          );
        }
      }
    } else {
      throw new ISE(
          "Found different granularities in segments %s",
          SegmentUtils.commaSeparatedIdentifiers(segments)
      );
    }
  }

  private boolean tryLockSegments(TaskActionClient actionClient, List<DataSegment> segments) throws IOException
  {
    final Map<Interval, List<DataSegment>> intervalToSegments = SegmentUtils.groupSegmentsByInterval(segments);
    final Closer lockCloserOnError = Closer.create();
    for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
      final Interval interval = entry.getKey();
      final List<DataSegment> segmentsInInterval = entry.getValue();
      final boolean hasSameVersion = segmentsInInterval
          .stream()
          .allMatch(segment -> segment.getVersion().equals(segmentsInInterval.get(0).getVersion()));
      Preconditions.checkState(
          hasSameVersion,
          "Segments %s should have same version",
          SegmentUtils.commaSeparatedIdentifiers(segmentsInInterval)
      );
      final List<LockResult> lockResults = actionClient.submit(
          new SegmentLockTryAcquireAction(
              TaskLockType.EXCLUSIVE,
              interval,
              segmentsInInterval.get(0).getVersion(),
              segmentsInInterval.stream()
                                .map(segment -> segment.getShardSpec().getPartitionNum())
                                .collect(Collectors.toSet())
          )
      );

      lockResults.stream()
                 .filter(LockResult::isOk)
                 .map(result -> (SegmentLock) result.getTaskLock())
                 .forEach(segmentLock -> lockCloserOnError.register(() -> actionClient.submit(
                     new SegmentLockReleaseAction(segmentLock.getInterval(), segmentLock.getPartitionId())
                 )));
      if (lockResults.stream().anyMatch(result -> !result.isOk())) {
        lockCloserOnError.close();
        return false;
      }
      lockedExistingSegments.addAll(segmentsInInterval);
      verifyAndFindRootPartitionRangeAndMinorVersion(segmentsInInterval);
    }
    return true;
  }

  /**
   * This method is called when the task overwrites existing segments with segment locks. It verifies the input segments
   * can be locked together, so that output segments can overshadow existing ones properly.
   * <p>
   * This method checks two things:
   * <p>
   * - Are rootPartition range of inputSegments adjacent? Two rootPartition ranges are adjacent if they are consecutive.
   * - All atomicUpdateGroups of inputSegments must be full. (See {@code AtomicUpdateGroup#isFull()}).
   */
  private void verifyAndFindRootPartitionRangeAndMinorVersion(List<DataSegment> inputSegments)
  {
    if (inputSegments.isEmpty()) {
      return;
    }

    final List<DataSegment> sortedSegments = new ArrayList<>(inputSegments);
    sortedSegments.sort((s1, s2) -> {
      if (s1.getStartRootPartitionId() != s2.getStartRootPartitionId()) {
        return Integer.compare(s1.getStartRootPartitionId(), s2.getStartRootPartitionId());
      } else {
        return Integer.compare(s1.getEndRootPartitionId(), s2.getEndRootPartitionId());
      }
    });
    verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(sortedSegments);
    final Interval interval = sortedSegments.get(0).getInterval();
    final short prevMaxMinorVersion = (short) sortedSegments
        .stream()
        .mapToInt(DataSegment::getMinorVersion)
        .max()
        .orElseThrow(() -> new ISE("Empty inputSegments"));

    overwritingRootGenPartitions.put(
        interval,
        new OverwritingRootGenerationPartitions(
            sortedSegments.get(0).getStartRootPartitionId(),
            sortedSegments.get(sortedSegments.size() - 1).getEndRootPartitionId(),
            prevMaxMinorVersion
        )
    );
  }

  public static void verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(List<DataSegment> sortedSegments)
  {
    if (sortedSegments.isEmpty()) {
      return;
    }

    Preconditions.checkArgument(
        sortedSegments.stream().allMatch(segment -> segment.getInterval().equals(sortedSegments.get(0).getInterval()))
    );

    short atomicUpdateGroupSize = 1;
    // sanity check
    for (int i = 0; i < sortedSegments.size() - 1; i++) {
      final DataSegment curSegment = sortedSegments.get(i);
      final DataSegment nextSegment = sortedSegments.get(i + 1);
      if (curSegment.getStartRootPartitionId() == nextSegment.getStartRootPartitionId()
          && curSegment.getEndRootPartitionId() == nextSegment.getEndRootPartitionId()) {
        // Input segments should have the same or consecutive rootPartition range
        if (curSegment.getMinorVersion() != nextSegment.getMinorVersion()
            || curSegment.getAtomicUpdateGroupSize() != nextSegment.getAtomicUpdateGroupSize()) {
          throw new ISE(
              "segment[%s] and segment[%s] have the same rootPartitionRange, but different minorVersion or atomicUpdateGroupSize",
              curSegment,
              nextSegment
          );
        }
        atomicUpdateGroupSize++;
      } else {
        if (curSegment.getEndRootPartitionId() != nextSegment.getStartRootPartitionId()) {
          throw new ISE("Can't compact segments of non-consecutive rootPartition range");
        }
        if (atomicUpdateGroupSize != curSegment.getAtomicUpdateGroupSize()) {
          throw new ISE("All atomicUpdateGroup must be compacted together");
        }
        atomicUpdateGroupSize = 1;
      }
    }
    if (atomicUpdateGroupSize != sortedSegments.get(sortedSegments.size() - 1).getAtomicUpdateGroupSize()) {
      throw new ISE("All atomicUpdateGroup must be compacted together");
    }
  }
}
