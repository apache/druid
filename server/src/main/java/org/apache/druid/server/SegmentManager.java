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
import org.apache.druid.query.DataSegmentAndDescriptor;
import org.apache.druid.query.LeafSegmentsBundle;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.ReferenceCountedIndexedTableProvider;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.metrics.SegmentRowCountDistribution;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.utils.CloseableUtils;
import org.apache.druid.utils.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
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
   * Returns a map of dataSource to the number of segments managed by this segmentManager.  This method should be used
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
  public Optional<VersionedIntervalTimeline<String, DataSegment>> getTimeline(TableDataSource dataSource)
  {
    return Optional.ofNullable(dataSources.get(dataSource.getName())).map(DataSourceState::getTimeline);
  }

  /**
   * Given a list of {@link DataSegmentAndDescriptor} produce a {@link LeafSegmentsBundle} which partitions segments
   * into cached, loadable, or missing segments. This gives callers the flexibilty to decide to perform operations
   * on segments which are already cached prior to or alongside the operation to load any segments which are not already
   * present in the cache on demand.
   * <p>
   * What this means mechanically, is that for each {@link DataSegmentAndDescriptor} we check if it is already cached
   * with {@link #acquireCachedSegment(DataSegment)} to add to {@link LeafSegmentsBundle#cachedSegments}, else if
   * {@link #canLoadSegmentOnDemand(DataSegment)} is true it is added to {@link LeafSegmentsBundle#loadableSegments} or
   * {@link LeafSegmentsBundle#missingSegments} if not.
   * <p>
   * The segments in {@link LeafSegmentsBundle#loadableSegments} can be retrieved with
   * {@link #acquireSegment(DataSegment)} to ensure they are loaded from deep storage.
   */
  public LeafSegmentsBundle getSegmentsBundle(
      List<DataSegmentAndDescriptor> segments,
      SegmentMapFunction segmentMapFunction
  )
  {
    final ArrayList<SegmentReference> segmentReferences = new ArrayList<>();
    final ArrayList<SegmentDescriptor> missingSegments = new ArrayList<>();
    final ArrayList<DataSegmentAndDescriptor> loadableSegments = new ArrayList<>();
    for (DataSegmentAndDescriptor segment : segments) {
      final DataSegment dataSegment = segment.getDataSegment();
      if (dataSegment == null) {
        missingSegments.add(segment.getDescriptor());
        continue;
      }
      Optional<Segment> ref = acquireCachedSegment(dataSegment);
      if (ref.isPresent()) {
        segmentReferences.add(
            new SegmentReference(
                segment.getDescriptor(),
                segmentMapFunction.apply(ref),
                null
            )
        );
      } else if (canLoadSegmentOnDemand(dataSegment)) {
        loadableSegments.add(segment);
      } else {
        missingSegments.add(segment.getDescriptor());
      }
    }
    return new LeafSegmentsBundle(segmentReferences, loadableSegments, missingSegments);
  }

  /**
   * Returns a {@link Segment} transformed with a {@link SegmentMapFunction}, if it is available in the cache. The
   * returned {@link Segment} must be closed when the caller is finished doing segment things. This method will not
   * download a {@link DataSegment} if it is not already present in {@link #cacheManager}, use
   * {@link #acquireSegment(DataSegment)} instead.
   */
  public Optional<Segment> acquireCachedSegment(DataSegment dataSegment)
  {
    return cacheManager.acquireCachedSegment(dataSegment);
  }

  /**
   * Returns a {@link AcquireSegmentAction}, where calling {@link AcquireSegmentAction#getSegmentFuture()} will either return
   * immediately if the {@link Segment} is in the cache, or possibly try to fetch the segment from deep storage if not.
   * The returned {@link Segment}, if present, must be closed when the caller is finished doing segment things.
   * <p>
   * Calling this method is treated as an intent to acquire and use the segment via resolving the future, and cache
   * manager implementations will place a hold on this segment until the 'loadCleanup' closer is closed - typically
   * after resolving the future to acquire the reference to the actual {@link Segment} object.
   */
  public AcquireSegmentAction acquireSegment(DataSegment dataSegment)
  {
    return cacheManager.acquireSegment(dataSegment);
  }

  /**
   * Returns the collection of {@link IndexedTable} for the entire timeline (since join conditions do not currently
   * consider the queries intervals), if the timeline exists for each of its segments that are joinable.
   */
  public Optional<Stream<ReferenceCountedIndexedTableProvider>> getIndexedTables(
      TableDataSource dataSource
  )
  {
    return getTimeline(dataSource).map(timeline -> {
      // join doesn't currently consider intervals, so just consider all segments
      final Stream<DataSegment> segments =
          timeline.lookup(Intervals.ETERNITY)
                  .stream()
                  .flatMap(x -> StreamSupport.stream(x.getObject().payloads().spliterator(), false));
      ConcurrentHashMap<SegmentId, ReferenceCountedIndexedTableProvider> tables =
          Optional.ofNullable(dataSources.get(dataSource.getName())).map(DataSourceState::getTablesLookup)
                  .orElseThrow(() -> new ISE("dataSource[%s] does not have IndexedTables", dataSource.getName()));
      return segments.map(segment -> tables.get(segment.getId())).filter(Objects::nonNull);
    });
  }

  public boolean hasIndexedTables(String dataSourceName)
  {
    if (dataSources.containsKey(dataSourceName)) {
      return !dataSources.get(dataSourceName).tablesLookup.isEmpty();
    }
    return false;
  }

  /**
   * Load the supplied segment into segment cache on bootstrap. If the segment is already loaded, this method does not
   * reload the segment into the segment cache.
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
    try {
      cacheManager.bootstrap(dataSegment, loadFailed);
    }
    catch (SegmentLoadingException e) {
      cacheManager.drop(dataSegment);
      throw e;
    }
    loadSegmentInternal(dataSegment);
  }


  /**
   * Load the supplied segment into segment cache. If the segment is already loaded, this method does not reload the
   * segment into the segment cache. This method should be called for non-bootstrapping flows. Unlike
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
    try {
      cacheManager.load(dataSegment);
    }
    catch (SegmentLoadingException e) {
      cacheManager.drop(dataSegment);
      throw e;
    }
    loadSegmentInternal(dataSegment);
  }

  private void loadSegmentInternal(
      final DataSegment dataSegment
  ) throws IOException
  {
    final SettableSupplier<Boolean> resultSupplier = new SettableSupplier<>();

    // compute() is used to ensure that the operation for a data source is executed atomically
    dataSources.compute(
        dataSegment.getDataSource(),
        (k, v) -> {
          final DataSourceState dataSourceState = v == null ? new DataSourceState() : v;
          final VersionedIntervalTimeline<String, DataSegment> loadedIntervals =
              dataSourceState.getTimeline();
          final PartitionChunk<DataSegment> entry = loadedIntervals.findChunk(
              dataSegment.getInterval(),
              dataSegment.getVersion(),
              dataSegment.getShardSpec().getPartitionNum()
          );

          if (entry != null) {
            log.warn("Told to load segment[%s] that already exists", dataSegment.getId());
            resultSupplier.set(false);
          } else {

            loadedIntervals.add(
                dataSegment.getInterval(),
                dataSegment.getVersion(),
                dataSegment.getShardSpec().createChunk(dataSegment)
            );

            long numOfRows = 0;
            final Optional<Segment> loadedSegment = cacheManager.acquireCachedSegment(dataSegment);
            if (loadedSegment.isPresent()) {
              final Segment segment = loadedSegment.get();
              final IndexedTable table = segment.as(IndexedTable.class);
              if (table != null) {
                if (dataSourceState.isEmpty() || dataSourceState.numSegments == dataSourceState.tablesLookup.size()) {
                  dataSourceState.tablesLookup.put(
                      segment.getId(),
                      new ReferenceCountedIndexedTableProvider(table)
                  );
                } else {
                  log.error(
                      "Cannot load segment[%s] with IndexedTable, no existing segments are joinable",
                      segment.getId()
                  );
                }
              } else if (!dataSourceState.tablesLookup.isEmpty()) {
                log.error(
                    "Cannot load segment[%s] without IndexedTable, all existing segments are joinable",
                    segment.getId()
                );
              }
              final PhysicalSegmentInspector countInspector = segment.as(PhysicalSegmentInspector.class);
              if (countInspector != null) {
                numOfRows = countInspector.getNumRows();
              }
              CloseableUtils.closeAndWrapExceptions(segment);
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

  public void dropSegment(final DataSegment dataSegment)
  {
    final String dataSource = dataSegment.getDataSource();

    // compute() is used to ensure that the operation for a data source is executed atomically
    dataSources.compute(
        dataSource,
        (dataSourceName, dataSourceState) -> {
          if (dataSourceState == null) {
            log.info("Told to delete a queryable for a dataSource[%s] that doesn't exist.", dataSourceName);
            return null;
          } else {
            final VersionedIntervalTimeline<String, DataSegment> loadedIntervals =
                dataSourceState.getTimeline();

            final PartitionChunk<DataSegment> removed = loadedIntervals.remove(
                dataSegment.getInterval(),
                dataSegment.getVersion(),
                // remove() internally searches for a partitionChunk to remove which is *equal* to the given
                // partitionChunk. Note that partitionChunk.equals() checks only the partitionNum, but not the object.
                dataSegment.getShardSpec().createChunk(dataSegment)
            );
            final DataSegment oldSegmentRef = (removed == null) ? null : removed.getObject();

            if (oldSegmentRef != null) {
              try (final Closer closer = Closer.create()) {
                final Optional<Segment> oldSegment = cacheManager.acquireCachedSegment(oldSegmentRef);
                long numberOfRows = oldSegment.map(segment -> {
                  closer.register(segment);
                  final PhysicalSegmentInspector countInspector = segment.as(PhysicalSegmentInspector.class);
                  if (countInspector != null) {
                    return countInspector.getNumRows();
                  }
                  return 0;
                }).orElse(0);

                dataSourceState.removeSegment(dataSegment, numberOfRows);

                log.info("Attempting to close segment[%s]", dataSegment.getId());
                final ReferenceCountedIndexedTableProvider oldTable = dataSourceState.tablesLookup.remove(dataSegment.getId());
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
                  dataSegment.getInterval(),
                  dataSegment.getVersion()
              );
            }

            // Returning null removes the entry of dataSource from the map
            return dataSourceState.isEmpty() ? null : dataSourceState;
          }
        }
    );

    cacheManager.removeInfoFile(dataSegment);
    cacheManager.drop(dataSegment);
  }

  /**
   * Return whether the cache manager can handle segments or not.
   */
  public boolean canHandleSegments()
  {
    return cacheManager.canHandleSegments();
  }

  public boolean canLoadSegmentsOnDemand()
  {
    return cacheManager.canLoadSegmentsOnDemand();
  }

  public boolean canLoadSegmentOnDemand(DataSegment dataSegment)
  {
    return cacheManager.canLoadSegmentOnDemand(dataSegment);
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

  public void shutdown()
  {
    cacheManager.shutdown();
  }


  /**
   * Represent the state of a data source including the timeline, total segment size, and number of segments.
   */
  public static class DataSourceState
  {
    private final VersionedIntervalTimeline<String, DataSegment> timeline =
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

    public VersionedIntervalTimeline<String, DataSegment> getTimeline()
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
