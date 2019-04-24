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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.SegmentLockTryAcquireAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
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

public abstract class AbstractTask implements Task
{
  private static final Joiner ID_JOINER = Joiner.on("_");

  @JsonIgnore
  private final String id;

  @JsonIgnore
  private final String groupId;

  @JsonIgnore
  private final TaskResource taskResource;

  @JsonIgnore
  private final String dataSource;

  private final Map<String, Object> context;

  @Nullable
  private Map<Interval, OverwritingRootGenerationPartitions> overwritingRootGenPartitions;

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

  protected AbstractTask(String id, String dataSource, Map<String, Object> context)
  {
    this(id, null, null, dataSource, context);
  }

  protected AbstractTask(
      String id,
      @Nullable String groupId,
      @Nullable TaskResource taskResource,
      String dataSource,
      @Nullable Map<String, Object> context
  )
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.groupId = groupId == null ? id : groupId;
    this.taskResource = taskResource == null ? new TaskResource(id, 1) : taskResource;
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.context = context == null ? new HashMap<>() : context;
  }

  public static String getOrMakeId(String id, final String typeName, String dataSource)
  {
    return getOrMakeId(id, typeName, dataSource, null);
  }

  static String getOrMakeId(String id, final String typeName, String dataSource, @Nullable Interval interval)
  {
    if (id != null) {
      return id;
    }

    final List<Object> objects = new ArrayList<>();
    objects.add(typeName);
    objects.add(dataSource);
    if (interval != null) {
      objects.add(interval.getStart());
      objects.add(interval.getEnd());
    }
    objects.add(DateTimes.nowUtc().toString());

    return joinId(objects);
  }

  @JsonProperty
  @Override
  public String getId()
  {
    return id;
  }

  @JsonProperty
  @Override
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty("resource")
  @Override
  public TaskResource getTaskResource()
  {
    return taskResource;
  }

  @Override
  public String getNodeType()
  {
    return null;
  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    return null;
  }

  @Override
  public String getClasspathPrefix()
  {
    return null;
  }

  @Override
  public boolean canRestore()
  {
    return false;
  }

  /**
   * Should be called independent of canRestore so that resource cleaning can be achieved.
   * If resource cleaning is required, concrete class should override this method
   */
  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
    // Do nothing and let the concrete class handle it
  }

  @Override
  public String toString()
  {
    return "AbstractTask{" +
           "id='" + id + '\'' +
           ", groupId='" + groupId + '\'' +
           ", taskResource=" + taskResource +
           ", dataSource='" + dataSource + '\'' +
           ", context=" + context +
           '}';
  }

  /**
   * Start helper methods
   *
   * @param objects objects to join
   *
   * @return string of joined objects
   */
  static String joinId(List<Object> objects)
  {
    return ID_JOINER.join(objects);
  }

  static String joinId(Object...objects)
  {
    return ID_JOINER.join(objects);
  }

  public TaskStatus success()
  {
    return TaskStatus.success(getId());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractTask that = (AbstractTask) o;

    if (!id.equals(that.id)) {
      return false;
    }

    if (!groupId.equals(that.groupId)) {
      return false;
    }

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }

    return context.equals(that.context);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(id, groupId, dataSource, context);
  }

  public static List<TaskLock> getTaskLocks(TaskActionClient client) throws IOException
  {
    return client.submit(new LockListAction());
  }

  @Override
  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  public abstract boolean requireLockInputSegments();

  public abstract List<DataSegment> findInputSegments(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException;

  public abstract boolean changeSegmentGranularity(List<Interval> intervalOfExistingSegments);

  /**
   * Returns the segmentGranularity for the given interval. Usually tasks are supposed to return its segmentGranularity
   * if exists. The compactionTask can return different segmentGranularity depending on its configuration and the input
   * interval.
   *
   * @return segmentGranularity or null if it doesn't support it.
   */
  @Nullable
  public abstract Granularity getSegmentGranularity(Interval interval);

  boolean tryLockWithIntervals(TaskActionClient client, Set<Interval> intervals)
      throws IOException
  {
    return tryLockWithIntervals(client, new ArrayList<>(intervals));
  }

  protected boolean tryLockWithIntervals(TaskActionClient client, List<Interval> intervals)
      throws IOException
  {
    if (requireLockInputSegments()) {
      // TODO: check changeSegmentGranularity and get timeChunkLock here

      // This method finds segments falling in all given intervals and then tries to lock those segments.
      // Thus, there might be a race between calling findInputSegments() and tryLockWithSegments(),
      // i.e., a new segment can be added to the interval or an existing segment might be removed.
      // Removed segments should be fine because indexing tasks would do nothing with removed segments.
      // However, tasks wouldn't know about new segments added after findInputSegments() call, it may missing those
      // segments. This is usually fine, but if you want to avoid this, you should use timeChunk lock instead.
      if (!intervals.isEmpty()) {
        return tryLockWithSegments(client, findInputSegments(client, intervals));
      } else {
        return true;
      }
    } else {
      changeSegmentGranularity = false;
      overwritingRootGenPartitions = Collections.emptyMap();
      return true;
    }
  }

  boolean tryLockWithSegments(TaskActionClient client, List<DataSegment> segments)
      throws IOException
  {
    if (requireLockInputSegments()) {
      if (segments.isEmpty()) {
        changeSegmentGranularity = false;
        overwritingRootGenPartitions = Collections.emptyMap();
        return true;
      }

      // Create a timeline to find latest segments only
      final List<Interval> intervals = segments.stream().map(DataSegment::getInterval).collect(Collectors.toList());

      changeSegmentGranularity = changeSegmentGranularity(intervals);
      if (changeSegmentGranularity) {
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

        final Map<Interval, List<DataSegment>> intervalToSegments = new HashMap<>();
        for (DataSegment segment : segmentsToLock) {
          intervalToSegments.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>()).add(segment);
        }
        intervalToSegments.values().forEach(this::verifyAndFindRootPartitionRangeAndMinorVersion);
        for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
          final Interval interval = entry.getKey();
          final Set<Integer> partitionIds = entry.getValue().stream()
                                                 .map(s -> s.getShardSpec().getPartitionNum())
                                                 .collect(Collectors.toSet());
          final List<LockResult> lockResults = client.submit(
              new SegmentLockTryAcquireAction(
                  TaskLockType.EXCLUSIVE,
                  interval,
                  entry.getValue().get(0).getVersion(),
                  partitionIds
              )
          );
          if (lockResults.isEmpty() || lockResults.stream().anyMatch(result -> !result.isOk())) {
            // TODO: unlock
            return false;
          }
        }
        return true;
      }
    } else {
      changeSegmentGranularity = false;
      overwritingRootGenPartitions = Collections.emptyMap();
      return true;
    }
  }

  /**
   * TODO: description about what this method verifies
   * TODO: similar check in taskLockbox??
   */
  private void verifyAndFindRootPartitionRangeAndMinorVersion(List<DataSegment> inputSegments)
  {
    if (inputSegments.isEmpty()) {
      return;
    }

    Preconditions.checkArgument(
        inputSegments.stream().allMatch(segment -> segment.getInterval().equals(inputSegments.get(0).getInterval()))
    );
    final Interval interval = inputSegments.get(0).getInterval();

    inputSegments.sort((s1, s2) -> {
      if (s1.getStartRootPartitionId() != s2.getStartRootPartitionId()) {
        return Integer.compare(s1.getStartRootPartitionId(), s2.getStartRootPartitionId());
      } else {
        return Integer.compare(s1.getEndRootPartitionId(), s2.getEndRootPartitionId());
      }
    });

    short atomicUpdateGroupSize = 1;
    // sanity check
    for (int i = 0; i < inputSegments.size() - 1; i++) {
      final DataSegment curSegment = inputSegments.get(i);
      final DataSegment nextSegment = inputSegments.get(i + 1);
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

    final short prevMaxMinorVersion = (short) inputSegments
        .stream()
        .mapToInt(DataSegment::getMinorVersion)
        .max()
        .orElseThrow(() -> new ISE("Empty inputSegments"));

    if (overwritingRootGenPartitions == null) {
      overwritingRootGenPartitions = new HashMap<>();
    }
    overwritingRootGenPartitions.put(
        interval,
        new OverwritingRootGenerationPartitions(
            inputSegments.get(0).getStartRootPartitionId(),
            inputSegments.get(inputSegments.size() - 1).getEndRootPartitionId(),
            prevMaxMinorVersion
        )
    );
  }

  protected boolean isChangeSegmentGranularity()
  {
    return Preconditions.checkNotNull(changeSegmentGranularity, "changeSegmentGranularity is not initialized");
  }

  Map<Interval, OverwritingRootGenerationPartitions> getAllOverwritingSegmentMeta()
  {
    Preconditions.checkNotNull(overwritingRootGenPartitions, "overwritingRootGenPartitions is not initialized");
    return Collections.unmodifiableMap(overwritingRootGenPartitions);
  }

  @Nullable
  public OverwritingRootGenerationPartitions getOverwritingSegmentMeta(Interval interval)
  {
    Preconditions.checkNotNull(overwritingRootGenPartitions, "overwritingRootGenPartitions is not initialized");
    return overwritingRootGenPartitions.get(interval);
  }

  public boolean isOverwriteMode()
  {
    Preconditions.checkNotNull(overwritingRootGenPartitions, "overwritingRootGenPartitions is not initialized");
    return !overwritingRootGenPartitions.isEmpty();
  }
}
