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

package org.apache.druid.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.ReferenceCountedIndexedTableProvider;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.metrics.SegmentRowCountDistribution;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.utils.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class is responsible for managing data sources and their states like timeline, total segment size, and number of
 * segments. All public methods of this class must be thread-safe.
 */
public class SegmentManager
{
  private static final EmittingLogger log = new EmittingLogger(SegmentManager.class);

  private final SegmentCacheManager cacheManager;

  private final ConcurrentHashMap<String, DataSourceState> dataSources = new ConcurrentHashMap<>();

  @Inject
  public SegmentManager(SegmentCacheManager cacheManager)
  {
    this.cacheManager = cacheManager;
  }

  @VisibleForTesting
  Map<String, DataSourceState> getDataSources()
  {
    return dataSources;
  }

  public Set<String> getDataSourceNames()
  {
    return dataSources.keySet();
  }

  /**
   * Returns a map of dataSource to the total byte size of segments managed by this segmentManager.  This method should
   * be used carefully because the returned map might be different from the actual data source states.
   *
   * @return a map of dataSources and their total byte sizes
   */
  public Map<String, Long> getDataSourceSizes()
  {
    return CollectionUtils.mapValues(dataSources, SegmentManager.DataSourceState::getTotalSegmentSize);
  }

  public Map<String, Long> getAverageRowCountForDatasource()
  {
    return CollectionUtils.mapValues(dataSources, SegmentManager.DataSourceState::getAverageRowCount);
  }

  public Map<String, SegmentRowCountDistribution> getRowCountDistribution()
  {
    return CollectionUtils.mapValues(dataSources, SegmentManager.DataSourceState::getSegmentRowCountDistribution);
  }

  /**
   * Returns a map of dataSource to the number of segments managed by this segmentManager.  This method should be
   * carefully because the returned map might be different from the actual data source states.
   *
   * @return a map of dataSources and number of segments
   */
  public Map<String, Long> getDataSourceCounts()
  {
    return CollectionUtils.mapValues(dataSources, SegmentManager.DataSourceState::getNumSegments);
  }

  /**
   * Returns the timeline for a datasource, if it exists. The analysis object passed in must represent a scan-based
   * datasource of a single table.
   */
  public Optional<VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider>> getTimeline(TableDataSource dataSource)
  {
    return Optional.ofNullable(dataSources.get(dataSource.getName())).map(DataSourceState::getTimeline);
  }

  /**
   * Returns the collection of {@link IndexedTable} for the entire timeline (since join conditions do not currently
   * consider the queries intervals), if the timeline exists for each of its segments that are joinable.
   */
  public Optional<Stream<ReferenceCountedIndexedTableProvider>> getIndexedTables(TableDataSource dataSource)
  {
    return getTimeline(dataSource).map(timeline -> {
      // join doesn't currently consider intervals, so just consider all segments
      final Stream<ReferenceCountedSegmentProvider> segments =
          timeline.lookup(Intervals.ETERNITY)
                  .stream()
                  .flatMap(x -> StreamSupport.stream(x.getObject().payloads().spliterator(), false));
      ConcurrentHashMap<SegmentId, ReferenceCountedIndexedTableProvider> tables =
          Optional.ofNullable(dataSources.get(dataSource.getName())).map(DataSourceState::getTablesLookup)
                  .orElseThrow(() -> new ISE("dataSource[%s] does not have IndexedTables", dataSource.getName()));
      return segments.map(segment -> tables.get(segment.getBaseSegment().getId())).filter(Objects::nonNull);
    });
  }

  public boolean hasIndexedTables(String dataSourceName)
  {
    if (dataSources.containsKey(dataSourceName)) {
      return dataSources.get(dataSourceName).tablesLookup.size() > 0;
    }
    return false;
  }

  /**
   * Load the supplied segment into page cache on bootstrap. If the segment is already loaded, this method does not
   * reload the segment into the page cache.
   *
   * @param dataSegment segment to bootstrap
   * @param loadFailed callback to execute when segment lazy load fails. This applies only
   *                   when lazy loading is enabled.
   *
   * @throws SegmentLoadingException if the segment cannot be loaded
   * @throws IOException if the segment info cannot be cached on disk
   */
  public void loadSegmentOnBootstrap(
      final DataSegment dataSegment,
      final SegmentLazyLoadFailCallback loadFailed
  ) throws SegmentLoadingException, IOException
  {
    final ReferenceCountedSegmentProvider segment;
    try {
      segment = cacheManager.getBootstrapSegment(dataSegment, loadFailed);
      if (segment == null) {
        throw new SegmentLoadingException(
            "No segment adapter found for bootstrap segment[%s] with loadSpec[%s].",
            dataSegment.getId(), dataSegment.getLoadSpec()
        );
      }
    }
    catch (SegmentLoadingException e) {
      cacheManager.cleanup(dataSegment);
      throw e;
    }
    loadSegmentInternal(dataSegment, segment);
  }


  /**
   * Load the supplied segment into page cache. If the segment is already loaded, this method does not reload the
   * segment into the page cache. This method should be called for non-bootstrapping flows. Unlike
   * {@link #loadSegmentOnBootstrap(DataSegment, SegmentLazyLoadFailCallback)}, this method doesn't accept a lazy load
   * fail callback because the segment is loaded immediately.
   *
   * @param dataSegment segment to load
   *
   * @throws SegmentLoadingException if the segment cannot be loaded
   * @throws IOException if the segment info cannot be cached on disk
   */
  public void loadSegment(final DataSegment dataSegment) throws SegmentLoadingException, IOException
  {
    final ReferenceCountedSegmentProvider segment;
    try {
      segment = cacheManager.getSegment(dataSegment);
      if (segment == null) {
        throw new SegmentLoadingException(
            "No segment adapter found for segment[%s] with loadSpec[%s].",
            dataSegment.getId(), dataSegment.getLoadSpec()
        );
      }
    }
    catch (SegmentLoadingException e) {
      cacheManager.cleanup(dataSegment);
      throw e;
    }
    loadSegmentInternal(dataSegment, segment);
  }

  private void loadSegmentInternal(
      final DataSegment dataSegment,
      final ReferenceCountedSegmentProvider segment
  ) throws IOException
  {
    final SettableSupplier<Boolean> resultSupplier = new SettableSupplier<>();

    // compute() is used to ensure that the operation for a data source is executed atomically
    dataSources.compute(
        dataSegment.getDataSource(),
        (k, v) -> {
          final DataSourceState dataSourceState = v == null ? new DataSourceState() : v;
          final VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider> loadedIntervals =
              dataSourceState.getTimeline();
          final PartitionChunk<ReferenceCountedSegmentProvider> entry = loadedIntervals.findChunk(
              dataSegment.getInterval(),
              dataSegment.getVersion(),
              dataSegment.getShardSpec().getPartitionNum()
          );

          if (entry != null) {
            log.warn("Told to load an adapter for segment[%s] that already exists", dataSegment.getId());
            resultSupplier.set(false);
          } else {
            final Segment baseSegment = segment.getBaseSegment();
            final IndexedTable table = baseSegment.as(IndexedTable.class);
            if (table != null) {
              if (dataSourceState.isEmpty() || dataSourceState.numSegments == dataSourceState.tablesLookup.size()) {
                dataSourceState.tablesLookup.put(baseSegment.getId(), new ReferenceCountedIndexedTableProvider(table));
              } else {
                log.error("Cannot load segment[%s] with IndexedTable, no existing segments are joinable", baseSegment.getId());
              }
            } else if (dataSourceState.tablesLookup.size() > 0) {
              log.error("Cannot load segment[%s] without IndexedTable, all existing segments are joinable", baseSegment.getId());
            }
            loadedIntervals.add(
                dataSegment.getInterval(),
                dataSegment.getVersion(),
                dataSegment.getShardSpec().createChunk(segment)
            );
            final PhysicalSegmentInspector countInspector = baseSegment.as(PhysicalSegmentInspector.class);
            final long numOfRows;
            if (dataSegment.isTombstone() || countInspector == null) {
              numOfRows = 0;
            } else {
              numOfRows = countInspector.getNumRows();
            }
            dataSourceState.addSegment(dataSegment, numOfRows);
            resultSupplier.set(true);
          }

          return dataSourceState;
        }
    );
    final boolean loadResult = resultSupplier.get();
    if (loadResult) {
      cacheManager.storeInfoFile(dataSegment);
    }
  }

  public void dropSegment(final DataSegment segment)
  {
    final String dataSource = segment.getDataSource();

    // compute() is used to ensure that the operation for a data source is executed atomically
    dataSources.compute(
        dataSource,
        (dataSourceName, dataSourceState) -> {
          if (dataSourceState == null) {
            log.info("Told to delete a queryable for a dataSource[%s] that doesn't exist.", dataSourceName);
            return null;
          } else {
            final VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider> loadedIntervals =
                dataSourceState.getTimeline();

            final ShardSpec shardSpec = segment.getShardSpec();
            final PartitionChunk<ReferenceCountedSegmentProvider> removed = loadedIntervals.remove(
                segment.getInterval(),
                segment.getVersion(),
                // remove() internally searches for a partitionChunk to remove which is *equal* to the given
                // partitionChunk. Note that partitionChunk.equals() checks only the partitionNum, but not the object.
                segment.getShardSpec().createChunk(ReferenceCountedSegmentProvider.wrapSegment(null, shardSpec))
            );
            final ReferenceCountedSegmentProvider oldSegmentRef = (removed == null) ? null : removed.getObject();

            if (oldSegmentRef != null) {
              try (final Closer closer = Closer.create()) {
                final PhysicalSegmentInspector countInspector = oldSegmentRef.getBaseSegment().as(PhysicalSegmentInspector.class);
                final long numOfRows;
                if (segment.isTombstone() || countInspector == null) {
                  numOfRows = 0;
                } else {
                  numOfRows = countInspector.getNumRows();
                }
                dataSourceState.removeSegment(segment, numOfRows);

                closer.register(oldSegmentRef);
                log.info("Attempting to close segment[%s]", segment.getId());
                final ReferenceCountedIndexedTableProvider oldTable = dataSourceState.tablesLookup.remove(segment.getId());
                if (oldTable != null) {
                  closer.register(oldTable);
                }
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            } else {
              log.info(
                  "Told to delete a queryable on dataSource[%s] for interval[%s] and version[%s] that I don't have.",
                  dataSourceName,
                  segment.getInterval(),
                  segment.getVersion()
              );
            }

            // Returning null removes the entry of dataSource from the map
            return dataSourceState.isEmpty() ? null : dataSourceState;
          }
        }
    );

    cacheManager.removeInfoFile(segment);
    cacheManager.cleanup(segment);
  }

  /**
   * Return whether the cache manager can handle segments or not.
   */
  public boolean canHandleSegments()
  {
    return cacheManager.canHandleSegments();
  }

  /**
   * Return a list of cached segments, if any. This should be called only when
   * {@link #canHandleSegments()} is true.
   */
  public List<DataSegment> getCachedSegments() throws IOException
  {
    return cacheManager.getCachedSegments();
  }

  /**
   * Shutdown the bootstrap executor to save resources.
   * This should be called after loading bootstrap segments into the page cache.
   */
  public void shutdownBootstrap()
  {
    cacheManager.shutdownBootstrap();
  }


  /**
   * Represent the state of a data source including the timeline, total segment size, and number of segments.
   */
  public static class DataSourceState
  {
    private final VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider> timeline =
        new VersionedIntervalTimeline<>(Ordering.natural());

    private final ConcurrentHashMap<SegmentId, ReferenceCountedIndexedTableProvider> tablesLookup = new ConcurrentHashMap<>();
    private long totalSegmentSize;
    private long numSegments;
    private long rowCount;
    private final SegmentRowCountDistribution segmentRowCountDistribution = new SegmentRowCountDistribution();

    private void addSegment(DataSegment segment, long numOfRows)
    {
      totalSegmentSize += segment.getSize();
      numSegments++;
      rowCount += (numOfRows);
      if (segment.isTombstone()) {
        segmentRowCountDistribution.addTombstoneToDistribution();
      } else {
        segmentRowCountDistribution.addRowCountToDistribution(numOfRows);
      }
    }

    private void removeSegment(DataSegment segment, long numOfRows)
    {
      totalSegmentSize -= segment.getSize();
      numSegments--;
      rowCount -= numOfRows;
      if (segment.isTombstone()) {
        segmentRowCountDistribution.removeTombstoneFromDistribution();
      } else {
        segmentRowCountDistribution.removeRowCountFromDistribution(numOfRows);
      }
    }

    public VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider> getTimeline()
    {
      return timeline;
    }

    public ConcurrentHashMap<SegmentId, ReferenceCountedIndexedTableProvider> getTablesLookup()
    {
      return tablesLookup;
    }

    public long getAverageRowCount()
    {
      return numSegments == 0 ? 0 : rowCount / numSegments;
    }

    public long getTotalSegmentSize()
    {
      return totalSegmentSize;
    }

    public long getNumSegments()
    {
      return numSegments;
    }

    public boolean isEmpty()
    {
      return numSegments == 0;
    }

    private SegmentRowCountDistribution getSegmentRowCountDistribution()
    {
      return segmentRowCountDistribution;
    }
  }
}
