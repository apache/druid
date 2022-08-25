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
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.ReferenceCountingIndexedTable;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.metrics.SegmentRowCountDistribution;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.utils.CollectionUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
  public static class DataSourceState
  {
    private final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline =
        new VersionedIntervalTimeline<>(Ordering.natural());

    private final ConcurrentHashMap<SegmentId, ReferenceCountingIndexedTable> tablesLookup = new ConcurrentHashMap<>();
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

    public VersionedIntervalTimeline<String, ReferenceCountingSegment> getTimeline()
    {
      return timeline;
    }

    public ConcurrentHashMap<SegmentId, ReferenceCountingIndexedTable> getTablesLookup()
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

  public Set<String> getDataSourceNames()
  {
    return dataSources.keySet();
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
   *
   * @param analysis data source analysis information
   *
   * @return timeline, if it exists
   *
   * @throws IllegalStateException if 'analysis' does not represent a scan-based datasource of a single table
   */
  public Optional<VersionedIntervalTimeline<String, ReferenceCountingSegment>> getTimeline(DataSourceAnalysis analysis)
  {
    final TableDataSource tableDataSource = getTableDataSource(analysis);
    return Optional.ofNullable(dataSources.get(tableDataSource.getName())).map(DataSourceState::getTimeline);
  }

  /**
   * Returns the collection of {@link IndexedTable} for the entire timeline (since join conditions do not currently
   * consider the queries intervals), if the timeline exists for each of its segments that are joinable.
   */
  public Optional<Stream<ReferenceCountingIndexedTable>> getIndexedTables(DataSourceAnalysis analysis)
  {
    return getTimeline(analysis).map(timeline -> {
      // join doesn't currently consider intervals, so just consider all segments
      final Stream<ReferenceCountingSegment> segments =
          timeline.lookup(Intervals.ETERNITY)
                  .stream()
                  .flatMap(x -> StreamSupport.stream(x.getObject().payloads().spliterator(), false));
      final TableDataSource tableDataSource = getTableDataSource(analysis);
      ConcurrentHashMap<SegmentId, ReferenceCountingIndexedTable> tables =
          Optional.ofNullable(dataSources.get(tableDataSource.getName())).map(DataSourceState::getTablesLookup)
                  .orElseThrow(() -> new ISE("Datasource %s does not have IndexedTables", tableDataSource.getName()));
      return segments.map(segment -> tables.get(segment.getId())).filter(Objects::nonNull);
    });
  }

  public boolean hasIndexedTables(String dataSourceName)
  {
    if (dataSources.containsKey(dataSourceName)) {
      return dataSources.get(dataSourceName).tablesLookup.size() > 0;
    }
    return false;
  }

  private TableDataSource getTableDataSource(DataSourceAnalysis analysis)
  {
    return analysis.getBaseTableDataSource()
                   .orElseThrow(() -> new ISE("Cannot handle datasource: %s", analysis.getDataSource()));
  }

  public boolean loadSegment(final DataSegment segment, boolean lazy, SegmentLazyLoadFailCallback loadFailed)
      throws SegmentLoadingException
  {
    return loadSegment(segment, lazy, loadFailed, null);
  }

  /**
   * Load a single segment.
   *
   * @param segment segment to load
   * @param lazy    whether to lazy load columns metadata
   * @param loadFailed callBack to execute when segment lazy load failed
   * @param loadSegmentIntoPageCacheExec If null is specified, the default thread pool in segment loader to load
   *                                     segments into page cache on download will be used. You can specify a dedicated
   *                                     thread pool of larger capacity when this function is called during historical
   *                                     process bootstrap to speed up initial loading.
   *
   * @return true if the segment was newly loaded, false if it was already loaded
   *
   * @throws SegmentLoadingException if the segment cannot be loaded
   */
  public boolean loadSegment(final DataSegment segment, boolean lazy, SegmentLazyLoadFailCallback loadFailed,
                             ExecutorService loadSegmentIntoPageCacheExec) throws SegmentLoadingException
  {
    final ReferenceCountingSegment adapter = getSegmentReference(segment, lazy, loadFailed);

    final SettableSupplier<Boolean> resultSupplier = new SettableSupplier<>();

    // compute() is used to ensure that the operation for a data source is executed atomically
    dataSources.compute(
        segment.getDataSource(),
        (k, v) -> {
          final DataSourceState dataSourceState = v == null ? new DataSourceState() : v;
          final VersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals =
              dataSourceState.getTimeline();
          final PartitionChunk<ReferenceCountingSegment> entry = loadedIntervals.findChunk(
              segment.getInterval(),
              segment.getVersion(),
              segment.getShardSpec().getPartitionNum()
          );

          if (entry != null) {
            log.warn("Told to load an adapter for segment[%s] that already exists", segment.getId());
            resultSupplier.set(false);
          } else {

            IndexedTable table = adapter.as(IndexedTable.class);
            if (table != null) {
              if (dataSourceState.isEmpty() || dataSourceState.numSegments == dataSourceState.tablesLookup.size()) {
                dataSourceState.tablesLookup.put(segment.getId(), new ReferenceCountingIndexedTable(table));
              } else {
                log.error("Cannot load segment[%s] with IndexedTable, no existing segments are joinable", segment.getId());
              }
            } else if (dataSourceState.tablesLookup.size() > 0) {
              log.error("Cannot load segment[%s] without IndexedTable, all existing segments are joinable", segment.getId());
            }
            loadedIntervals.add(
                segment.getInterval(),
                segment.getVersion(),
                segment.getShardSpec().createChunk(adapter)
            );
            StorageAdapter storageAdapter = adapter.asStorageAdapter();
            long numOfRows = (segment.isTombstone() || storageAdapter == null) ? 0 : storageAdapter.getNumRows();
            dataSourceState.addSegment(segment, numOfRows);
            // Asyncly load segment index files into page cache in a thread pool
            segmentLoader.loadSegmentIntoPageCache(segment, loadSegmentIntoPageCacheExec);
            resultSupplier.set(true);

          }

          return dataSourceState;
        }
    );

    return resultSupplier.get();
  }

  private ReferenceCountingSegment getSegmentReference(final DataSegment dataSegment, boolean lazy, SegmentLazyLoadFailCallback loadFailed) throws SegmentLoadingException
  {
    final ReferenceCountingSegment segment;
    try {
      segment = segmentLoader.getSegment(dataSegment, lazy, loadFailed);
    }
    catch (SegmentLoadingException e) {
      segmentLoader.cleanup(dataSegment);
      throw e;
    }

    if (segment == null) {
      throw new SegmentLoadingException("Null adapter from loadSpec[%s]", dataSegment.getLoadSpec());
    }
    return segment;
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
            final VersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals =
                dataSourceState.getTimeline();

            final ShardSpec shardSpec = segment.getShardSpec();
            final PartitionChunk<ReferenceCountingSegment> removed = loadedIntervals.remove(
                segment.getInterval(),
                segment.getVersion(),
                // remove() internally searches for a partitionChunk to remove which is *equal* to the given
                // partitionChunk. Note that partitionChunk.equals() checks only the partitionNum, but not the object.
                segment.getShardSpec().createChunk(ReferenceCountingSegment.wrapSegment(null, shardSpec))
            );
            final ReferenceCountingSegment oldQueryable = (removed == null) ? null : removed.getObject();


            if (oldQueryable != null) {
              try (final Closer closer = Closer.create()) {
                StorageAdapter storageAdapter = oldQueryable.asStorageAdapter();
                long numOfRows = (segment.isTombstone() || storageAdapter == null) ? 0 : storageAdapter.getNumRows();
                dataSourceState.removeSegment(segment, numOfRows);

                closer.register(oldQueryable);
                log.info("Attempting to close segment %s", segment.getId());
                final ReferenceCountingIndexedTable oldTable = dataSourceState.tablesLookup.remove(segment.getId());
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

    segmentLoader.cleanup(segment);
  }
}
