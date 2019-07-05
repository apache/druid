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
import org.apache.druid.indexing.common.SegmentLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.SegmentListUsedAction;
import org.apache.druid.indexing.common.actions.SegmentLockReleaseAction;
import org.apache.druid.indexing.common.actions.SegmentLockTryAcquireAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import org.apache.druid.indexing.firehose.WindowedSegmentId;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.io.Closer;
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
import java.util.Set;
import java.util.stream.Collectors;

public class SegmentLockHelper
{
  private final String dataSource;
  private final Map<Interval, OverwritingRootGenerationPartitions> overwritingRootGenPartitions = new HashMap<>();
  private final Set<DataSegment> lockedExistingSegments = new HashSet<>();
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

  public SegmentLockHelper(String dataSource)
  {
    this.dataSource = dataSource;
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

  @Nullable
  public Granularity getKnownSegmentGranularity()
  {
    return knownSegmentGranularity;
  }

  public boolean tryLockExistingSegments(
      TaskActionClient actionClient,
      List<Interval> intervalsToFind // intervals must be aligned with the segment granularity of existing segments
  ) throws IOException
  {
    final List<DataSegment> segments = actionClient.submit(
        new SegmentListUsedAction(
            dataSource,
            null,
            intervalsToFind
        )
    );
    return verifyAndLockExistingSegments(actionClient, segments);
  }

  public boolean tryLockExistingSegments(
      TaskActionClient actionClient,
      IngestSegmentFirehoseFactory firehoseFactory
  ) throws IOException
  {
    return verifyAndLockExistingSegments(actionClient, findExistingSegments(actionClient, firehoseFactory));
  }

  public boolean verifyAndLockExistingSegments(TaskActionClient actionClient, List<DataSegment> segments)
      throws IOException
  {
    final List<DataSegment> segmentsToLock = segments.stream()
                                                     .filter(segment -> !lockedExistingSegments.contains(segment))
                                                     .collect(Collectors.toList());
    if (segmentsToLock.isEmpty()) {
      return true;
    }

    verifySegmentGranularity(segmentsToLock);
    return tryLockSegmentsIfNeeded(actionClient, segmentsToLock);
  }

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
              "Non-aligned segments[%s] for granularity[%s]",
              nonAlignedSegments.stream().map(DataSegment::getId).collect(Collectors.toList()),
              knownSegmentGranularity
          );
        }
      }
    } else {
      throw new ISE(
          "Found different granularities in segments[%s]",
          segments.stream().map(DataSegment::getId).collect(Collectors.toList())
      );
    }
  }

  private boolean tryLockSegmentsIfNeeded(TaskActionClient actionClient, List<DataSegment> segments) throws IOException
  {
    final Map<Interval, List<DataSegment>> intervalToSegments = groupSegmentsByInterval(segments);
    final Closer lockCloserOnError = Closer.create();
    for (List<DataSegment> segmentsInInterval : intervalToSegments.values()) {
      for (DataSegment segment : segmentsInInterval) {
        final List<LockResult> lockResults = actionClient.submit(
            new SegmentLockTryAcquireAction(
                TaskLockType.EXCLUSIVE,
                segment.getInterval(),
                segment.getVersion(), Collections.singleton(segment.getShardSpec().getPartitionNum())
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
        lockedExistingSegments.add(segment);
      }
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

  private List<DataSegment> findExistingSegments(
      TaskActionClient actionClient,
      IngestSegmentFirehoseFactory firehoseFactory
  ) throws IOException
  {
    final List<WindowedSegmentId> inputSegments = firehoseFactory.getSegments();
    if (inputSegments == null) {
      final Interval inputInterval = Preconditions.checkNotNull(firehoseFactory.getInterval(), "input interval");

      return actionClient.submit(
          new SegmentListUsedAction(dataSource, null, Collections.singletonList(inputInterval))
      );
    } else {
      final List<String> inputSegmentIds = inputSegments.stream()
                                                        .map(WindowedSegmentId::getSegmentId)
                                                        .collect(Collectors.toList());
      final Set<DataSegment> dataSegmentsInIntervals = new HashSet<>(
          actionClient.submit(
              new SegmentListUsedAction(
                  dataSource,
                  null,
                  inputSegments.stream()
                               .flatMap(windowedSegmentId -> windowedSegmentId.getIntervals().stream())
                               .collect(Collectors.toSet())
              )
          )
      );
      return dataSegmentsInIntervals.stream()
                                    .filter(segment -> inputSegmentIds.contains(segment.getId().toString()))
                                    .collect(Collectors.toList());
    }
  }

  private static Map<Interval, List<DataSegment>> groupSegmentsByInterval(List<DataSegment> segments)
  {
    final Map<Interval, List<DataSegment>> map = new HashMap<>();
    for (DataSegment segment : segments) {
      map.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>()).add(segment);
    }
    return map;
  }

  private static Granularity granularityOfInterval(Interval interval)
  {
    return GranularityType.fromPeriod(interval.toPeriod()).getDefaultGranularity();
  }
}
