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
import com.google.common.collect.Iterables;
import org.apache.druid.indexing.common.SegmentLock;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.SegmentLockReleaseAction;
import org.apache.druid.indexing.common.actions.SegmentLockTryAcquireAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
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
import java.util.stream.StreamSupport;

public abstract class AbstractBatchIndexTask extends AbstractTask
{
  @Nullable
  private Map<Interval, OverwritingRootGenerationPartitions> overwritingRootGenPartitions;
  @Nullable
  private Set<DataSegment> allInputSegments;

  @Nullable
  private Boolean changeSegmentGranularity;

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

  protected AbstractBatchIndexTask(String id, String dataSource, Map<String, Object> context)
  {
    super(id, dataSource, context);
  }

  protected AbstractBatchIndexTask(
      String id,
      @Nullable String groupId,
      @Nullable TaskResource taskResource,
      String dataSource,
      @Nullable Map<String, Object> context
  )
  {
    super(id, groupId, taskResource, dataSource, context);
  }

  public abstract boolean requireLockInputSegments();

  public abstract List<DataSegment> findInputSegments(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException;

  public abstract boolean changeSegmentGranularity(List<Interval> intervalOfExistingSegments);

  public abstract boolean isPerfectRollup();

  /**
   * Returns the segmentGranularity for the given interval. Usually tasks are supposed to return its segmentGranularity
   * if exists. The compactionTask can return different segmentGranularity depending on its configuration and the input
   * interval.
   *
   * @return segmentGranularity or null if it doesn't support it.
   */
  @Nullable
  public abstract Granularity getSegmentGranularity(Interval interval);

  protected boolean tryLockWithIntervals(TaskActionClient client, List<Interval> intervals, boolean isInitialRequest)
      throws IOException
  {
    if (requireLockInputSegments()) {
      if (isPerfectRollup()) {
        return tryTimeChunkLock(client, intervals);
      } else if (!intervals.isEmpty()) {
        // This method finds segments falling in all given intervals and then tries to lock those segments.
        // Thus, there might be a race between calling findInputSegments() and tryLockWithSegments(),
        // i.e., a new segment can be added to the interval or an existing segment might be removed.
        // Removed segments should be fine because indexing tasks would do nothing with removed segments.
        // However, tasks wouldn't know about new segments added after findInputSegments() call, it may missing those
        // segments. This is usually fine, but if you want to avoid this, you should use timeChunk lock instead.
        return tryLockWithSegments(client, findInputSegments(client, intervals), isInitialRequest);
      } else {
        return true;
      }
    } else {
      changeSegmentGranularity = false;
      allInputSegments = Collections.emptySet();
      overwritingRootGenPartitions = Collections.emptyMap();
      return true;
    }
  }

  boolean tryTimeChunkLock(TaskActionClient client, List<Interval> intervals) throws IOException
  {
    allInputSegments = Collections.emptySet();
    overwritingRootGenPartitions = Collections.emptyMap();
    // In this case, the intervals to lock must be alighed with segmentGranularity if it's defined
    final Set<Interval> uniqueIntervals = new HashSet<>();
    for (Interval interval : JodaUtils.condenseIntervals(intervals)) {
      final Granularity segmentGranularity = getSegmentGranularity(interval);
      if (segmentGranularity == null) {
        uniqueIntervals.add(interval);
      } else {
        Iterables.addAll(uniqueIntervals, segmentGranularity.getIterable(interval));
      }
    }

    for (Interval interval : uniqueIntervals) {
      final TaskLock lock = client.submit(new TimeChunkLockTryAcquireAction(TaskLockType.EXCLUSIVE, interval));
      if (lock == null) {
        return false;
      }
    }
    return true;
  }

  boolean tryLockWithSegments(TaskActionClient client, List<DataSegment> segments, boolean isInitialRequest) throws IOException
  {
    if (segments.isEmpty()) {
      changeSegmentGranularity = false;
      allInputSegments = Collections.emptySet();
      overwritingRootGenPartitions = Collections.emptyMap();
      return true;
    }

    if (requireLockInputSegments()) {
      // Create a timeline to find latest segments only
      final List<Interval> intervals = segments.stream().map(DataSegment::getInterval).collect(Collectors.toList());

      changeSegmentGranularity = changeSegmentGranularity(intervals);
      if (changeSegmentGranularity) {
        return tryTimeChunkLock(client, intervals);
      } else {
        final List<DataSegment> segmentsToLock;
        final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(
            segments
        );
        segmentsToLock = timeline.lookup(JodaUtils.umbrellaInterval(intervals))
                                 .stream()
                                 .map(TimelineObjectHolder::getObject)
                                 .flatMap(partitionHolder -> StreamSupport.stream(
                                     partitionHolder.spliterator(),
                                     false
                                 ))
                                 .map(PartitionChunk::getObject)
                                 .collect(Collectors.toList());

        if (allInputSegments == null) {
          allInputSegments = new HashSet<>(segmentsToLock);
          overwritingRootGenPartitions = new HashMap<>();
        }

        final Map<Interval, List<DataSegment>> intervalToSegments = new HashMap<>();
        for (DataSegment segment : segmentsToLock) {
          intervalToSegments.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>()).add(segment);
        }
        intervalToSegments.values().forEach(
            segmentsToCheck -> verifyAndFindRootPartitionRangeAndMinorVersion(segmentsToCheck, isInitialRequest)
        );
        final Closer lockCloserOnError = Closer.create();
        for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
          final Interval interval = entry.getKey();
          final Set<Integer> partitionIds = entry.getValue().stream()
                                                 .map(s -> s.getShardSpec().getPartitionNum())
                                                 .collect(Collectors.toSet());
          final List<LockResult> lockResults = client.submit(
              new SegmentLockTryAcquireAction(
                  TaskLockType.EXCLUSIVE,
                  interval,
                  entry.getValue().get(0).getMajorVersion(),
                  partitionIds
              )
          );

          lockResults.stream()
                     .filter(LockResult::isOk)
                     .map(result -> (SegmentLock) result.getTaskLock())
                     .forEach(segmentLock -> lockCloserOnError.register(() -> client.submit(
                         new SegmentLockReleaseAction(segmentLock.getInterval(), segmentLock.getPartitionId())
                     )));

          if (isInitialRequest && (lockResults.isEmpty() || lockResults.stream().anyMatch(result -> !result.isOk()))) {
            lockCloserOnError.close();
            return false;
          }
        }
        return true;
      }
    } else {
      changeSegmentGranularity = false;
      allInputSegments = Collections.emptySet();
      overwritingRootGenPartitions = Collections.emptyMap();
      return true;
    }
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
  private void verifyAndFindRootPartitionRangeAndMinorVersion(List<DataSegment> inputSegments, boolean isInitialRequest)
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
    if (isInitialRequest) {
      verifyRootPartitionIsAdjacentAndAtomicUpdateGroupIsFull(sortedSegments);
    }
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

  public Set<DataSegment> getAllInputSegments()
  {
    return Preconditions.checkNotNull(allInputSegments, "allInputSegments is not initialized");
  }

  Map<Interval, OverwritingRootGenerationPartitions> getAllOverwritingSegmentMeta()
  {
    Preconditions.checkNotNull(overwritingRootGenPartitions, "overwritingRootGenPartitions is not initialized");
    return Collections.unmodifiableMap(overwritingRootGenPartitions);
  }

  public boolean needMinorOverwrite()
  {
    return hasInputSegments() && !isChangeSegmentGranularity();
  }

  public boolean isChangeSegmentGranularity()
  {
    return Preconditions.checkNotNull(changeSegmentGranularity, "changeSegmentGranularity is not initialized");
  }

  private boolean hasInputSegments()
  {
    Preconditions.checkNotNull(overwritingRootGenPartitions, "overwritingRootGenPartitions is not initialized");
    return !overwritingRootGenPartitions.isEmpty();
  }

  @Nullable
  public OverwritingRootGenerationPartitions getOverwritingSegmentMeta(Interval interval)
  {
    Preconditions.checkNotNull(overwritingRootGenPartitions, "overwritingRootGenPartitions is not initialized");
    return overwritingRootGenPartitions.get(interval);
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
