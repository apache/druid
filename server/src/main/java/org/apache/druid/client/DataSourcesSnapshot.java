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
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An immutable snapshot information about used segments and overshadowed segments for
 * {@link org.apache.druid.metadata.SQLMetadataSegmentManager}.
 */
public class DataSourcesSnapshot
{
  public static DataSourcesSnapshot fromUsedSegments(
      Iterable<DataSegment> segments,
      ImmutableMap<String, String> dataSourceProperties
  )
  {
    Map<String, DruidDataSource> dataSources = new HashMap<>();
    segments.forEach(segment -> {
      dataSources
          .computeIfAbsent(segment.getDataSource(), dsName -> new DruidDataSource(dsName, dataSourceProperties))
          .addSegmentIfAbsent(segment);
    });
    return new DataSourcesSnapshot(CollectionUtils.mapValues(dataSources, DruidDataSource::toImmutableDruidDataSource));
  }

  public static DataSourcesSnapshot fromUsedSegmentsTimelines(
      Map<String, VersionedIntervalTimeline<String, DataSegment>> usedSegmentsTimelinesPerDataSource,
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
    return new DataSourcesSnapshot(dataSourcesWithAllUsedSegments, usedSegmentsTimelinesPerDataSource);
  }

  private final Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments;
  private final Map<String, VersionedIntervalTimeline<String, DataSegment>> usedSegmentsTimelinesPerDataSource;
  private final ImmutableSet<SegmentId> overshadowedSegments;

  public DataSourcesSnapshot(Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments)
  {
    this(
        dataSourcesWithAllUsedSegments,
        CollectionUtils.mapValues(
            dataSourcesWithAllUsedSegments,
            dataSource -> VersionedIntervalTimeline.forSegments(dataSource.getSegments())
        )
    );
  }

  private DataSourcesSnapshot(
      Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments,
      Map<String, VersionedIntervalTimeline<String, DataSegment>> usedSegmentsTimelinesPerDataSource
  )
  {
    this.dataSourcesWithAllUsedSegments = dataSourcesWithAllUsedSegments;
    this.usedSegmentsTimelinesPerDataSource = usedSegmentsTimelinesPerDataSource;
    this.overshadowedSegments = ImmutableSet.copyOf(determineOvershadowedSegments());
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

  public Map<String, VersionedIntervalTimeline<String, DataSegment>> getUsedSegmentsTimelinesPerDataSource()
  {
    return usedSegmentsTimelinesPerDataSource;
  }

  public ImmutableSet<SegmentId> getOvershadowedSegments()
  {
    return overshadowedSegments;
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
   * https://github.com/apache/incubator-druid/issues/8070.
   *
   * @return overshadowed segment Ids list
   */
  private List<SegmentId> determineOvershadowedSegments()
  {
    // It's fine to add all overshadowed segments to a single collection because only
    // a small fraction of the segments in the cluster are expected to be overshadowed,
    // so building this collection shouldn't generate a lot of garbage.
    final List<SegmentId> overshadowedSegments = new ArrayList<>();
    for (ImmutableDruidDataSource dataSource : dataSourcesWithAllUsedSegments.values()) {
      VersionedIntervalTimeline<String, DataSegment> usedSegmentsTimeline =
          usedSegmentsTimelinesPerDataSource.get(dataSource.getName());
      for (DataSegment segment : dataSource.getSegments()) {
        if (usedSegmentsTimeline.isOvershadowed(segment.getInterval(), segment.getVersion(), segment)) {
          overshadowedSegments.add(segment.getId());
        }
      }
    }
    return overshadowedSegments;
  }

}
