/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.loading.SegmentLoader;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class is responsible for managing data sources and their states like timeline, total segment size, and number of
 * segments.  All public methods of this class must be thread-safe.
 */
public class SegmentManager
{
  private static final EmittingLogger log = new EmittingLogger(SegmentManager.class);

  private final SegmentLoader segmentLoader;
  private final ConcurrentHashMap<String, DataSourceState> dataSources = new ConcurrentHashMap<>();

  /**
   * Represent the state of a data source including the timeline, total segment size, and number of segments.
   */
  static class DataSourceState
  {
    private final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline =
        new VersionedIntervalTimeline<>(Ordering.natural());
    private long totalSegmentSize;
    private long numSegments;

    void addSegment(DataSegment segment)
    {
      totalSegmentSize += segment.getSize();
      numSegments++;
    }

    void removeSegment(DataSegment segment)
    {
      totalSegmentSize -= segment.getSize();
      numSegments--;
    }

    VersionedIntervalTimeline<String, ReferenceCountingSegment> getTimeline()
    {
      return timeline;
    }

    long getTotalSegmentSize()
    {
      return totalSegmentSize;
    }

    long getNumSegments()
    {
      return numSegments;
    }
  }

  @Inject
  public SegmentManager(
      SegmentLoader segmentLoader
  )
  {
    this.segmentLoader = segmentLoader;
  }

  @VisibleForTesting
  Map<String, DataSourceState> getDataSources()
  {
    return dataSources;
  }

  public Map<String, Long> getDataSourceSizes()
  {
    return dataSources.entrySet().stream()
                      .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getTotalSegmentSize()));
  }

  public Map<String, Long> getDataSourceCounts()
  {
    return dataSources.entrySet().stream()
                      .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getNumSegments()));
  }

  public boolean isSegmentCached(final DataSegment segment) throws SegmentLoadingException
  {
    return segmentLoader.isSegmentLoaded(segment);
  }

  public VersionedIntervalTimeline<String, ReferenceCountingSegment> getTimeline(String dataSource)
  {
    return dataSources.get(dataSource).getTimeline();
  }

  /**
   * Load a single segment.
   *
   * @param segment segment to load
   *
   * @return true if the segment was newly loaded, false if it was already loaded
   *
   * @throws SegmentLoadingException if the segment cannot be loaded
   */
  public boolean loadSegment(final DataSegment segment) throws SegmentLoadingException
  {
    final Segment adapter = getAdapter(segment);

    final DataSourceState computeResult = dataSources.compute(
        segment.getDataSource(),
        (k, v) -> {
          final DataSourceState dataSourceState = v == null ? new DataSourceState() : v;
          final VersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals =
              dataSourceState.getTimeline();
          final PartitionHolder<ReferenceCountingSegment> entry = loadedIntervals.findEntry(
              segment.getInterval(),
              segment.getVersion()
          );

          if ((entry != null) && (entry.getChunk(segment.getShardSpec().getPartitionNum()) != null)) {
            log.warn("Told to load a adapter for a segment[%s] that already exists", segment.getIdentifier());
            return null;
          } else {
            loadedIntervals.add(
                segment.getInterval(),
                segment.getVersion(),
                segment.getShardSpec().createChunk(new ReferenceCountingSegment(adapter))
            );
            dataSourceState.addSegment(segment);
            return dataSourceState;
          }
        }
    );

    return computeResult != null;
  }

  private Segment getAdapter(final DataSegment segment) throws SegmentLoadingException
  {
    final Segment adapter;
    try {
      adapter = segmentLoader.getSegment(segment);
    }
    catch (SegmentLoadingException e) {
      try {
        segmentLoader.cleanup(segment);
      }
      catch (SegmentLoadingException e1) {
        e.addSuppressed(e1);
      }
      throw e;
    }

    if (adapter == null) {
      throw new SegmentLoadingException("Null adapter from loadSpec[%s]", segment.getLoadSpec());
    }
    return adapter;
  }

  public void dropSegment(final DataSegment segment) throws SegmentLoadingException
  {
    final String dataSource = segment.getDataSource();

    dataSources.compute(
        dataSource,
        (k, v) -> {
          final DataSourceState dataSourceState = v == null ? new DataSourceState() : v;

          final VersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals =
              dataSourceState.getTimeline();

          if (loadedIntervals == null) {
            log.info("Told to delete a queryable for a dataSource[%s] that doesn't exist.", dataSource);
            return null;
          }

          final PartitionChunk<ReferenceCountingSegment> removed = loadedIntervals.remove(
              segment.getInterval(),
              segment.getVersion(),
              segment.getShardSpec().createChunk(null)
          );
          final ReferenceCountingSegment oldQueryable = (removed == null) ? null : removed.getObject();

          if (oldQueryable != null) {
            dataSourceState.removeSegment(segment);

            try {
              log.info("Attempting to close segment %s", segment.getIdentifier());
              oldQueryable.close();
            }
            catch (IOException e) {
              log.makeAlert(e, "Exception closing segment")
                 .addData("dataSource", dataSource)
                 .addData("segmentId", segment.getIdentifier())
                 .emit();
            }
          } else {
            log.info(
                "Told to delete a queryable on dataSource[%s] for interval[%s] and version [%s] that I don't have.",
                dataSource,
                segment.getInterval(),
                segment.getVersion()
            );
          }

          return dataSourceState;
        }
    );

    segmentLoader.cleanup(segment);
  }
}
