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

package org.apache.druid.server.coordinator.compact;

import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Implementation of {@link CompactionSegmentIterator} that returns segments in
 * order of their priority.
 */
public class PriorityBasedCompactionSegmentIterator implements CompactionSegmentIterator
{
  private static final Logger log = new Logger(PriorityBasedCompactionSegmentIterator.class);

  private final PriorityQueue<SegmentsToCompact> queue;
  private final Map<String, DataSourceCompactibleSegmentIterator> datasourceIterators;

  public PriorityBasedCompactionSegmentIterator(
      Map<String, DataSourceCompactionConfig> compactionConfigs,
      Map<String, SegmentTimeline> datasourceToTimeline,
      Map<String, List<Interval>> skipIntervals,
      @Nullable String priorityDatasource,
      Comparator<SegmentsToCompact> segmentPriority,
      CompactionStatusTracker statusTracker
  )
  {
    final Comparator<SegmentsToCompact> comparator;
    if (priorityDatasource == null) {
      comparator = segmentPriority;
    } else {
      comparator = Ordering.compound(
          Arrays.asList(
              Ordering.explicit(priorityDatasource).onResultOf(entry -> entry.getFirst().getDataSource()),
              segmentPriority
          )
      );
    }
    this.queue = new PriorityQueue<>(comparator);

    this.datasourceIterators = Maps.newHashMapWithExpectedSize(datasourceToTimeline.size());
    compactionConfigs.forEach((datasource, config) -> {
      if (config == null) {
        throw DruidException.defensive("Invalid null compaction config for dataSource[%s].", datasource);
      }
      final SegmentTimeline timeline = datasourceToTimeline.get(datasource);
      if (timeline == null) {
        log.warn("Skipping compaction for datasource[%s] as it has no timeline.", datasource);
        return;
      }

      datasourceIterators.put(
          datasource,
          new DataSourceCompactibleSegmentIterator(
              compactionConfigs.get(datasource),
              timeline,
              skipIntervals.getOrDefault(datasource, Collections.emptyList()),
              segmentPriority,
              statusTracker
          )
      );
      addNextItemForDatasourceToQueue(datasource);
    });
  }

  @Override
  public Map<String, CompactionStatistics> totalCompactedStatistics()
  {
    return CollectionUtils.mapValues(
        datasourceIterators,
        DataSourceCompactibleSegmentIterator::totalCompactedStatistics
    );
  }

  @Override
  public Map<String, CompactionStatistics> totalSkippedStatistics()
  {
    return CollectionUtils.mapValues(
        datasourceIterators,
        DataSourceCompactibleSegmentIterator::totalSkippedStatistics
    );
  }

  @Override
  public boolean hasNext()
  {
    return !queue.isEmpty();
  }

  @Override
  public SegmentsToCompact next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final SegmentsToCompact entry = queue.poll();
    if (entry == null || entry.isEmpty()) {
      throw new NoSuchElementException();
    }

    addNextItemForDatasourceToQueue(entry.getFirst().getDataSource());
    return entry;
  }

  private void addNextItemForDatasourceToQueue(String dataSourceName)
  {
    final DataSourceCompactibleSegmentIterator iterator = datasourceIterators.get(dataSourceName);
    if (iterator.hasNext()) {
      final SegmentsToCompact segmentsToCompact = iterator.next();
      if (!segmentsToCompact.isEmpty()) {
        queue.add(segmentsToCompact);
      }
    }
  }
}
