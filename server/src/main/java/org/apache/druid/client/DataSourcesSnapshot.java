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
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
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
import java.util.Set;

/**
 * An immutable snapshot of metadata information about used segments and overshadowed segments, coming from
 * {@link SqlSegmentsMetadataManager}.
 */
public class DataSourcesSnapshot
{
  public static DataSourcesSnapshot fromUsedSegments(
      Iterable<DataSegment> segments,
      ImmutableMap<String, String> dataSourceProperties
  )
  {
    return fromUsedSegments(segments, dataSourceProperties, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());
  }

  public static DataSourcesSnapshot fromUsedSegments(
      Iterable<DataSegment> segments,
      ImmutableMap<String, String> dataSourceProperties,
      Map<String, ImmutableDruidDataSource> previousDataSourcesWithAllUsedSegments,
      Map<String, CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChangeRequest>>>> previousDataSourceChanges,
      Map<String, Map<SegmentId, Boolean>> handedOffState
  )
  {
    Map<String, DruidDataSource> dataSources = new HashMap<>();
    segments.forEach(segment -> dataSources
        .computeIfAbsent(segment.getDataSource(), dsName -> new DruidDataSource(dsName, dataSourceProperties))
        .addSegmentIfAbsent(segment));
    Map<String, ImmutableDruidDataSource> immutableDruidDataSources = CollectionUtils.mapValues(dataSources, DruidDataSource::toImmutableDruidDataSource);
    Map<String, CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChangeRequest>>>> changes = computeDiff(previousDataSourcesWithAllUsedSegments, immutableDruidDataSources, previousDataSourceChanges);
    return new DataSourcesSnapshot(immutableDruidDataSources, handedOffState, changes);
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
        ImmutableMap.of()
    );
  }

  private static final int CHANGES_QUEUE_MAX_SIZE = 10;
  private final Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments;
  private final Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource;
  private final ImmutableSet<DataSegment> overshadowedSegments;

  private final Map<String, Map<SegmentId, Boolean>> handedOffStatePerDataSource;

  private final Map<String, CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChangeRequest>>>> dataSourceChanges;

  public DataSourcesSnapshot(
      Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments
  ) {
    this(dataSourcesWithAllUsedSegments, ImmutableMap.of(), ImmutableMap.of());
  }

  public DataSourcesSnapshot(
      Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments,
      Map<String, Map<SegmentId, Boolean>> handedOffStatePerDataSource,
      Map<String, CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChangeRequest>>>> dataSourceChanges
      )
  {
    this(
        dataSourcesWithAllUsedSegments,
        CollectionUtils.mapValues(
            dataSourcesWithAllUsedSegments,
            dataSource -> SegmentTimeline.forSegments(dataSource.getSegments())
        ),
        handedOffStatePerDataSource,
        dataSourceChanges
    );
  }

  private DataSourcesSnapshot(
      Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments,
      Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource,
      Map<String, Map<SegmentId, Boolean>> handedOffStatePerDataSource,
      Map<String, CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChangeRequest>>>> dataSourceChanges
  )
  {
    this.dataSourcesWithAllUsedSegments = dataSourcesWithAllUsedSegments;
    this.usedSegmentsTimelinesPerDataSource = usedSegmentsTimelinesPerDataSource;
    this.overshadowedSegments = ImmutableSet.copyOf(determineOvershadowedSegments());
    this.handedOffStatePerDataSource = handedOffStatePerDataSource;
    this.dataSourceChanges = dataSourceChanges;
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

  public Map<String, Map<SegmentId, Boolean>> getHandedOffStatePerDataSource()
  {
    return handedOffStatePerDataSource;
  }

  public Map<String, CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChangeRequest>>>> getDataSourceChanges() {
    return dataSourceChanges;
  }

  public Map<String, ChangeRequestsSnapshot<DataSegmentChangeRequest>> getChangesSince(
      Map<String, ChangeRequestHistory.Counter> counterPerDataSource) {
    Map<String, ChangeRequestsSnapshot<DataSegmentChangeRequest>> changesSincePerDataSource = new HashMap<>();

    counterPerDataSource.forEach((dataSource, counter) -> {
      if (!dataSourceChanges.containsKey(dataSource) || counter.getCounter() < 0) {
        changesSincePerDataSource.put(dataSource, ChangeRequestsSnapshot.fail(""));
      }
      CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChangeRequest>>> buffer = dataSourceChanges.get(dataSource);
      ChangeRequestHistory.Counter lastCounter = getLastCounter(buffer);
      if (counter.getCounter() == lastCounter.getCounter()) {
        if (counter.matches(lastCounter)) {
          changesSincePerDataSource.put(dataSource, ChangeRequestsSnapshot.success(counter, new ArrayList<>()));
        } else {
          changesSincePerDataSource.put(dataSource, ChangeRequestsSnapshot.fail(""));
        }
      } else if (counter.getCounter() > lastCounter.getCounter() || lastCounter.getCounter() - counter.getCounter() >= CHANGES_QUEUE_MAX_SIZE) {
        changesSincePerDataSource.put(dataSource, ChangeRequestsSnapshot.fail(""));
      } else {
        int changeStartIndex = (int) (counter.getCounter() + buffer.size() - lastCounter.getCounter());

        ChangeRequestHistory.Counter counterToMatch = counter.getCounter() == 0 ? ChangeRequestHistory.Counter.ZERO : buffer.get(changeStartIndex - 1).getCounter();

        if (!counterToMatch.matches(counter)) {
          changesSincePerDataSource.put(dataSource, ChangeRequestsSnapshot.fail(""));
        }

        List<DataSegmentChangeRequest> result = new ArrayList<>();
        for (int i = changeStartIndex; i < buffer.size(); i++) {
          result.addAll(buffer.get(i).getChangeRequest());
        }

        changesSincePerDataSource.put(dataSource, ChangeRequestsSnapshot.success(buffer.get(buffer.size() - 1).getCounter(), result));
      }

    });

    return changesSincePerDataSource;
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
  private List<DataSegment> determineOvershadowedSegments()
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
    return overshadowedSegments;
  }

  private static Map<String, CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChangeRequest>>>> computeDiff(
      Map<String, ImmutableDruidDataSource> previousDataSourcesWithAllUsedSegments,
      Map<String, ImmutableDruidDataSource> currentDataSourcesWithAllUsedSegments,
      Map<String, CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChangeRequest>>>> changes
  ) {
    Set<String> dataSources = new HashSet<>(previousDataSourcesWithAllUsedSegments.keySet());
    dataSources.addAll(currentDataSourcesWithAllUsedSegments.keySet());

    dataSources.forEach(dataSource -> {
      Map<SegmentId, DataSegment> previousDataSegments = (previousDataSourcesWithAllUsedSegments.containsKey(dataSource) ? previousDataSourcesWithAllUsedSegments.get(dataSource).getIdToSegments() : new HashMap<>());
      Map<SegmentId, DataSegment> currentDataSegments = (currentDataSourcesWithAllUsedSegments.containsKey(dataSource) ? currentDataSourcesWithAllUsedSegments.get(dataSource).getIdToSegments() : new HashMap<>());

      Set<SegmentId> segmentIdsToBeRemoved = Sets.difference(previousDataSegments.keySet(), currentDataSegments.keySet());
      Set<SegmentId> segmentIdsToBeAdded = Sets.difference(currentDataSegments.keySet(), previousDataSegments.keySet());

      List<DataSegmentChangeRequest> changeList = new ArrayList<>();
      segmentIdsToBeRemoved.forEach(segmentId -> changeList.add(new SegmentChangeRequestDrop(previousDataSegments.get(segmentId))));
      segmentIdsToBeAdded.forEach(segmentId -> changeList.add(new SegmentChangeRequestLoad(currentDataSegments.get(segmentId))));

      CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChangeRequest>>> buffer = changes.computeIfAbsent(dataSource, v -> new CircularBuffer<>(CHANGES_QUEUE_MAX_SIZE));

      ChangeRequestHistory.Counter lastCounter = getLastCounter(buffer);
      buffer.add(new ChangeRequestHistory.Holder<>(changeList, lastCounter.inc()));
    });

    changes.entrySet().removeIf(item -> item.getValue() == null || item.getValue().size() == 0);

    return changes;
  }

  private static ChangeRequestHistory.Counter getLastCounter(CircularBuffer<ChangeRequestHistory.Holder<List<DataSegmentChangeRequest>>> buffer) {
    return buffer.size() > 0 ? buffer.get(buffer.size() - 1).getCounter() : ChangeRequestHistory.Counter.ZERO;
  }
}
