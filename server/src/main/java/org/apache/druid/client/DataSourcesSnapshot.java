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

package org.apache.druid.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegmentChange;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.SegmentWithOvershadowedStatus;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.utils.CircularBuffer;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An immutable snapshot of metadata information about used segments and overshadowed segments, coming from
 * {@link SqlSegmentsMetadataManager}.
 */
public class DataSourcesSnapshot
{
  private static final EmittingLogger log = new EmittingLogger(DataSourcesSnapshot.class);

  public static DataSourcesSnapshot fromUsedSegments(
      Iterable<DataSegment> segments,
      ImmutableMap<String, String> dataSourceProperties
  )
  {
    return fromUsedSegments(
        segments,
        dataSourceProperties,
        ImmutableMap.of(),
        ImmutableSet.of(),
        new CircularBuffer<>(CHANGES_QUEUE_MAX_SIZE)
    );
  }

  public static DataSourcesSnapshot fromUsedSegments(
      Iterable<DataSegment> segments,
      ImmutableMap<String, String> dataSourceProperties,
      Map<String, Set<SegmentId>> handedOffState
  )
  {
    return fromUsedSegments(
        segments,
        dataSourceProperties,
        handedOffState,
        ImmutableSet.of(),
        new CircularBuffer<>(CHANGES_QUEUE_MAX_SIZE)
    );
  }

  public static DataSourcesSnapshot fromUsedSegments(
      Iterable<DataSegment> segments,
      ImmutableMap<String, String> dataSourceProperties,
      Map<String, Set<SegmentId>> handedOffState,
      Set<SegmentWithOvershadowedStatus> oldSegments,
      CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> oldChanges
  )
  {
    Map<String, DruidDataSource> dataSources = new HashMap<>();
    segments.forEach(segment -> dataSources
        .computeIfAbsent(segment.getDataSource(), dsName -> new DruidDataSource(dsName, dataSourceProperties))
        .addSegmentIfAbsent(segment));
    Map<String, ImmutableDruidDataSource> immutableDruidDataSources = CollectionUtils.mapValues(dataSources, DruidDataSource::toImmutableDruidDataSource);
    Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource = CollectionUtils.mapValues(
        immutableDruidDataSources,
        dataSource -> SegmentTimeline.forSegments(dataSource.getSegments())
    );
    ImmutableSet<DataSegment> overshadowedSegments = determineOvershadowedSegments(immutableDruidDataSources, usedSegmentsTimelinesPerDataSource);
    CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> changes =
        computeChanges(
            oldSegments,
            getSegmentsWithOvershadowedStatus(immutableDruidDataSources.values(), overshadowedSegments, handedOffState),
            oldChanges);
    return new DataSourcesSnapshot(immutableDruidDataSources, usedSegmentsTimelinesPerDataSource, overshadowedSegments, handedOffState, changes);
  }

  public static DataSourcesSnapshot fromUsedSegmentsTimelines(
      Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource,
      ImmutableMap<String, String> dataSourceProperties
  )
  {
    Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments =
        Maps.newHashMapWithExpectedSize(usedSegmentsTimelinesPerDataSource.size());
    usedSegmentsTimelinesPerDataSource.forEach(
        (dataSourceName, usedSegmentsTimeline) -> {
          DruidDataSource dataSource = new DruidDataSource(dataSourceName, dataSourceProperties);
          usedSegmentsTimeline.iterateAllObjects().forEach(dataSource::addSegment);
          dataSourcesWithAllUsedSegments.put(dataSourceName, dataSource.toImmutableDruidDataSource());
        }
    );
    return new DataSourcesSnapshot(
        dataSourcesWithAllUsedSegments,
        usedSegmentsTimelinesPerDataSource,
        ImmutableMap.of(),
        new CircularBuffer<>(CHANGES_QUEUE_MAX_SIZE)
    );
  }

  private static final int CHANGES_QUEUE_MAX_SIZE = 10;
  private final Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments;
  private final Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource;
  private final ImmutableSet<DataSegment> overshadowedSegments;

  private final Map<String, Set<SegmentId>> handedOffStatePerDataSource;

  private final CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> changes;

  public DataSourcesSnapshot(
      Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments
  )
  {
    this(
        dataSourcesWithAllUsedSegments,
        CollectionUtils.mapValues(
            dataSourcesWithAllUsedSegments,
            dataSource -> SegmentTimeline.forSegments(dataSource.getSegments())
        ),
        ImmutableMap.of(),
        new CircularBuffer<>(CHANGES_QUEUE_MAX_SIZE)
    );
  }

  private DataSourcesSnapshot(
      Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments,
      Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource,
      Map<String, Set<SegmentId>> handedOffStatePerDataSource,
      CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> changes
  )
  {
    this(
        dataSourcesWithAllUsedSegments,
        usedSegmentsTimelinesPerDataSource,
        determineOvershadowedSegments(dataSourcesWithAllUsedSegments, usedSegmentsTimelinesPerDataSource),
        handedOffStatePerDataSource,
        changes
    );
  }

  private DataSourcesSnapshot(
      Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments,
      Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource,
      ImmutableSet<DataSegment> overshadowedSegments,
      Map<String, Set<SegmentId>> handedOffStatePerDataSource,
      CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> changes
  )
  {
    this.dataSourcesWithAllUsedSegments = dataSourcesWithAllUsedSegments;
    this.usedSegmentsTimelinesPerDataSource = usedSegmentsTimelinesPerDataSource;
    this.overshadowedSegments = overshadowedSegments;
    this.handedOffStatePerDataSource = handedOffStatePerDataSource;
    this.changes = changes;
  }

  public Collection<ImmutableDruidDataSource> getDataSourcesWithAllUsedSegments()
  {
    return dataSourcesWithAllUsedSegments.values();
  }

  public Map<String, ImmutableDruidDataSource> getDataSourcesMap()
  {
    return dataSourcesWithAllUsedSegments;
  }

  @Nullable
  public ImmutableDruidDataSource getDataSource(String dataSourceName)
  {
    return dataSourcesWithAllUsedSegments.get(dataSourceName);
  }

  public Map<String, SegmentTimeline> getUsedSegmentsTimelinesPerDataSource()
  {
    return usedSegmentsTimelinesPerDataSource;
  }

  public ImmutableSet<DataSegment> getOvershadowedSegments()
  {
    return overshadowedSegments;
  }

  public Map<String, Set<SegmentId>> getHandedOffStatePerDataSource()
  {
    return handedOffStatePerDataSource;
  }

  public CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> getChanges()
  {
    return changes;
  }

  public ChangeRequestsSnapshot<DataSegmentChange> getChangesSince(ChangeRequestHistory.Counter counter)
  {
    if (counter.getCounter() < 0) {
      return ChangeRequestsSnapshot.fail(StringUtils.format("counter[%s] must be >= 0", counter));
    }

    ChangeRequestHistory.Counter lastCounter = getLastCounter(changes);

    if (counter.getCounter() == lastCounter.getCounter()) {
      if (counter.matches(lastCounter)) {
        return ChangeRequestsSnapshot.success(counter, new ArrayList<>());
      } else {
        return ChangeRequestsSnapshot.fail(StringUtils.format("counter[%s] failed to match with [%s]", counter, lastCounter));
      }
    } else if (counter.getCounter() > lastCounter.getCounter()) {
      return ChangeRequestsSnapshot.fail(
          StringUtils.format(
            "counter[%s] > last counter[%s]",
            counter,
            lastCounter
          )
      );
    } else if (lastCounter.getCounter() - counter.getCounter() >= CHANGES_QUEUE_MAX_SIZE) {
      return ChangeRequestsSnapshot.fail(
          StringUtils.format(
              "can't serve request, not enough history is kept. given counter [%s] and current last counter [%s]",
              counter,
              lastCounter
          )
      );
    } else {
      int changeStartIndex = (int) (counter.getCounter() + changes.size() - lastCounter.getCounter());

      ChangeRequestHistory.Counter counterToMatch = counter.getCounter() == 0 ? ChangeRequestHistory.Counter.ZERO : changes.get(changeStartIndex - 1).getCounter();

      if (!counterToMatch.matches(counter)) {
        return ChangeRequestsSnapshot.fail(
            StringUtils.format(
                "counter[%s] failed to match with [%s]",
                counter,
                lastCounter
            )
        );
      }

      List<DataSegmentChange> result = new ArrayList<>();
      for (int i = changeStartIndex; i < changes.size(); i++) {
        result.addAll(changes.get(i).getChangeRequest());
      }

      return ChangeRequestsSnapshot.success(changes.get(changes.size() - 1).getCounter(), result);
    }
  }

  /**
   * Returns an iterable to go over all used segments in all data sources. The order in which segments are iterated
   * is unspecified.
   *
   * Note: the iteration may not be as trivially cheap as, for example, iteration over an ArrayList. Try (to some
   * reasonable extent) to organize the code so that it iterates the returned iterable only once rather than several
   * times.
   *
   * This method's name starts with "iterate" because the result is expected to be consumed immediately in a for-each
   * statement or a stream pipeline, like
   * for (DataSegment segment : snapshot.iterateAllUsedSegmentsInSnapshot()) {...}
   */
  public Iterable<DataSegment> iterateAllUsedSegmentsInSnapshot()
  {
    return () -> dataSourcesWithAllUsedSegments
        .values()
        .stream()
        .flatMap(dataSource -> dataSource.getSegments().stream())
        .iterator();
  }

  /**
   * This method builds timelines from all data sources and finds the overshadowed segments list
   *
   * This method should be deduplicated with {@link VersionedIntervalTimeline#findFullyOvershadowed()}: see
   * https://github.com/apache/druid/issues/8070.
   *
   * @return List of overshadowed segments
   */
  private static ImmutableSet<DataSegment> determineOvershadowedSegments(
      Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments,
      Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource)
  {
    // It's fine to add all overshadowed segments to a single collection because only
    // a small fraction of the segments in the cluster are expected to be overshadowed,
    // so building this collection shouldn't generate a lot of garbage.
    final List<DataSegment> overshadowedSegments = new ArrayList<>();
    for (ImmutableDruidDataSource dataSource : dataSourcesWithAllUsedSegments.values()) {
      SegmentTimeline usedSegmentsTimeline =
          usedSegmentsTimelinesPerDataSource.get(dataSource.getName());
      for (DataSegment segment : dataSource.getSegments()) {
        if (usedSegmentsTimeline.isOvershadowed(segment)) {
          overshadowedSegments.add(segment);
        }
      }
    }
    return ImmutableSet.copyOf(overshadowedSegments);
  }

  public static Set<SegmentWithOvershadowedStatus> getSegmentsWithOvershadowedStatus(
      Collection<ImmutableDruidDataSource> segments,
      Set<DataSegment> overshadowedSegments,
      Map<String, Set<SegmentId>> handedOffState)
  {

    final Stream<DataSegment> usedSegments = segments
        .stream()
        .flatMap(t -> t.getSegments().stream());

    return usedSegments
        .map(segment -> new SegmentWithOvershadowedStatus(
            segment,
            overshadowedSegments.contains(segment),
            getHandedOffStateForSegment(handedOffState, segment.getDataSource(), segment.getId())
             )
        )
        .collect(Collectors.toSet());
  }

  private static boolean getHandedOffStateForSegment(
      Map<String, Set<SegmentId>> handedOffState,
      String dataSource, SegmentId segmentId
  )
  {
    return handedOffState
        .getOrDefault(dataSource, new HashSet<>())
        .contains(segmentId);
  }

  private static CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> computeChanges(
      Set<SegmentWithOvershadowedStatus> oldSegments,
      Set<SegmentWithOvershadowedStatus> currentSegments,
      CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> oldChanges
  )
  {
    if (oldSegments.isEmpty()) {
      return new CircularBuffer<>(CHANGES_QUEUE_MAX_SIZE);
    }

    // a segment is added to the change set, if following changes:
    // segmentId
    // overshadowed state
    // handed off state

    Map<SegmentId, SegmentWithOvershadowedStatus> oldSegmentsMap =
        oldSegments
            .stream()
            .collect(Collectors.toMap(
                segment -> segment.getDataSegment().getId(),
                Function.identity()));

    Map<SegmentId, SegmentWithOvershadowedStatus> currentSegmentsMap =
        currentSegments
            .stream()
            .collect(Collectors.toMap(
                segment -> segment.getDataSegment().getId(),
                Function.identity()));

    Set<SegmentWithOvershadowedStatus> segmentToBeRemoved = Sets.difference(oldSegments, currentSegments);
    Set<SegmentWithOvershadowedStatus> segmentToBeAdded = Sets.difference(currentSegments, oldSegments);

    List<DataSegmentChange> changeList = new ArrayList<>();

    segmentToBeRemoved.forEach(segment -> {
      SegmentWithOvershadowedStatus newSegment = currentSegmentsMap.get(segment.getDataSegment().getId());
      if (null == newSegment) {
        changeList.add(new DataSegmentChange(segment, false, DataSegmentChange.ChangeReason.SEGMENT_REMOVED));
      }
    });

    segmentToBeAdded.forEach(segment -> {
      SegmentWithOvershadowedStatus oldSegment = oldSegmentsMap.get(segment.getDataSegment().getId());
      boolean handedOffStatusChanged = !Objects.equals(null != oldSegment && oldSegment.isHandedOff(), segment.isHandedOff());
      boolean overshadowedStatusChanged = !Objects.equals(null != oldSegment && oldSegment.isOvershadowed(), segment.isOvershadowed());
      DataSegmentChange.ChangeReason changeReason;

      if (null == oldSegment) {
        changeReason = DataSegmentChange.ChangeReason.SEGMENT_ADDED;
      } else if (handedOffStatusChanged && overshadowedStatusChanged) {
        changeReason = DataSegmentChange.ChangeReason.SEGMENT_OVERSHADOWED_AND_HANDED_OFF;
      } else if (handedOffStatusChanged) {
        changeReason = DataSegmentChange.ChangeReason.SEGMENT_HANDED_OFF;
      } else {
        changeReason = DataSegmentChange.ChangeReason.SEGMENT_OVERSHADOWED;
      }
      changeList.add(new DataSegmentChange(segment, true, changeReason));
    });

    ChangeRequestHistory.Counter lastCounter = getLastCounter(oldChanges);

    if (changeList.size() > 0) {
      oldChanges.add(new ChangeRequestHistory.Holder<>(changeList, lastCounter.inc()));
    }

    log.info(
        "Finished computing segment changes. Changes count [%d], current counter [%d]",
        changeList.size(), getLastCounter(oldChanges).getCounter());
    return oldChanges;
  }

  public static ChangeRequestHistory.Counter getLastCounter(CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChange>>> buffer)
  {
    return buffer.size() > 0 ? buffer.get(buffer.size() - 1).getCounter() : ChangeRequestHistory.Counter.ZERO;
  }
}
