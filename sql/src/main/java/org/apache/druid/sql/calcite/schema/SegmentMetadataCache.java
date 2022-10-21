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

package org.apache.druid.sql.calcite.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.client.BrokerInternalQueryConfig;
import org.apache.druid.client.ServerView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.metadata.metadata.AllColumnIncluderator;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.SegmentMetadataCacheConfig;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Broker-side cache of segment metadata which combines segments to identify
 * datasources which become "tables" in Calcite. This cache provides the "physical"
 * metadata about a datasource which is blended with catalog "logical" metadata
 * to provide the final user-view of each datasource.
 */
@ManageLifecycle
public class SegmentMetadataCache
{
  // Newest segments first, so they override older ones.
  private static final Comparator<SegmentId> SEGMENT_ORDER = Comparator
      .comparing((SegmentId segmentId) -> segmentId.getInterval().getStart())
      .reversed()
      .thenComparing(Function.identity());

  private static final EmittingLogger log = new EmittingLogger(SegmentMetadataCache.class);
  private static final int MAX_SEGMENTS_PER_QUERY = 15000;
  private static final long DEFAULT_NUM_ROWS = 0;

  private final QueryLifecycleFactory queryLifecycleFactory;
  private final SegmentMetadataCacheConfig config;
  // Escalator, so we can attach an authentication result to queries we generate.
  private final Escalator escalator;
  private final SegmentManager segmentManager;
  private final JoinableFactory joinableFactory;
  private final ExecutorService cacheExec;
  private final ExecutorService callbackExec;

  /**
   * Map of DataSource -> DruidTable.
   * This map can be accessed by {@link #cacheExec} and {@link #callbackExec} threads.
   */
  private final ConcurrentMap<String, DatasourceTable.PhysicalDatasourceMetadata> tables = new ConcurrentHashMap<>();

  private static final Interner<RowSignature> ROW_SIGNATURE_INTERNER = Interners.newWeakInterner();

  /**
   * DataSource -> Segment -> AvailableSegmentMetadata(contains RowSignature) for that segment.
   * Use SortedMap for segments so they are merged in deterministic order, from older to newer.
   *
   * This map is updated by these two threads.
   *
   * - {@link #callbackExec} can update it in {@link #addSegment}, {@link #removeServerSegment},
   *   and {@link #removeSegment}.
   * - {@link #cacheExec} can update it in {@link #refreshSegmentsForDataSource}.
   *
   * While it is being updated, this map is read by these two types of thread.
   *
   * - {@link #cacheExec} can iterate all {@link AvailableSegmentMetadata}s per datasource.
   *   See {@link #buildDruidTable}.
   * - Query threads can create a snapshot of the entire map for processing queries on the system table.
   *   See {@link #getSegmentMetadataSnapshot()}.
   *
   * As the access pattern of this map is read-intensive, we should minimize the contention between writers and readers.
   * Since there are two threads that can update this map at the same time, those writers should lock the inner map
   * first and then lock the entry before it updates segment metadata. This can be done using
   * {@link ConcurrentMap#compute} as below. Note that, if you need to update the variables guarded by {@link #lock}
   * inside of compute(), you should get the lock before calling compute() to keep the function executed in compute()
   * not expensive.
   *
   * <pre>
   *   segmentMedataInfo.compute(
   *     datasourceParam,
   *     (datasource, segmentsMap) -> {
   *       if (segmentsMap == null) return null;
   *       else {
   *         segmentsMap.compute(
   *           segmentIdParam,
   *           (segmentId, segmentMetadata) -> {
   *             // update segmentMetadata
   *           }
   *         );
   *         return segmentsMap;
   *       }
   *     }
   *   );
   * </pre>
   *
   * Readers can simply delegate the locking to the concurrent map and iterate map entries.
   */
  private final ConcurrentHashMap<String, ConcurrentSkipListMap<SegmentId, AvailableSegmentMetadata>> segmentMetadataInfo
      = new ConcurrentHashMap<>();

  // For awaitInitialization.
  private final CountDownLatch initialized = new CountDownLatch(1);

  /**
   * This lock coordinates the access from multiple threads to those variables guarded by this lock.
   * Currently, there are 2 threads that can access these variables.
   *
   * - {@link #callbackExec} executes the timeline callbacks whenever BrokerServerView changes.
   * - {@link #cacheExec} periodically refreshes segment metadata and {@link DatasourceTable} if necessary
   *   based on the information collected via timeline callbacks.
   */
  private final Object lock = new Object();

  // All mutable segments.
  @GuardedBy("lock")
  private final TreeSet<SegmentId> mutableSegments = new TreeSet<>(SEGMENT_ORDER);

  // All dataSources that need tables regenerated.
  @GuardedBy("lock")
  private final Set<String> dataSourcesNeedingRebuild = new HashSet<>();

  // All segments that need to be refreshed.
  @GuardedBy("lock")
  private final TreeSet<SegmentId> segmentsNeedingRefresh = new TreeSet<>(SEGMENT_ORDER);

  // Configured context to attach to internally generated queries.
  private final BrokerInternalQueryConfig brokerInternalQueryConfig;

  @GuardedBy("lock")
  private boolean refreshImmediately = false;

  @GuardedBy("lock")
  private boolean isServerViewInitialized = false;

  /**
   * Counts the total number of known segments. This variable is used only for the segments table in the system schema
   * to initialize a map with a more proper size when it creates a snapshot. As a result, it doesn't have to be exact,
   * and thus there is no concurrency control for this variable.
   */
  private int totalSegments = 0;

  @Inject
  public SegmentMetadataCache(
      final QueryLifecycleFactory queryLifecycleFactory,
      final TimelineServerView serverView,
      final SegmentManager segmentManager,
      final JoinableFactory joinableFactory,
      final SegmentMetadataCacheConfig config,
      final Escalator escalator,
      final BrokerInternalQueryConfig brokerInternalQueryConfig
  )
  {
    this.queryLifecycleFactory = Preconditions.checkNotNull(queryLifecycleFactory, "queryLifecycleFactory");
    Preconditions.checkNotNull(serverView, "serverView");
    this.segmentManager = segmentManager;
    this.joinableFactory = joinableFactory;
    this.config = Preconditions.checkNotNull(config, "config");
    this.cacheExec = Execs.singleThreaded("DruidSchema-Cache-%d");
    this.callbackExec = Execs.singleThreaded("DruidSchema-Callback-%d");
    this.escalator = escalator;
    this.brokerInternalQueryConfig = brokerInternalQueryConfig;

    initServerViewTimelineCallback(serverView);
  }

  private void initServerViewTimelineCallback(final TimelineServerView serverView)
  {
    serverView.registerTimelineCallback(
        callbackExec,
        new TimelineServerView.TimelineCallback()
        {
          @Override
          public ServerView.CallbackAction timelineInitialized()
          {
            synchronized (lock) {
              isServerViewInitialized = true;
              lock.notifyAll();
            }

            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentAdded(final DruidServerMetadata server, final DataSegment segment)
          {
            addSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(final DataSegment segment)
          {
            removeSegment(segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction serverSegmentRemoved(
              final DruidServerMetadata server,
              final DataSegment segment
          )
          {
            removeServerSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  private void startCacheExec()
  {
    cacheExec.submit(
        () -> {
          long lastRefresh = 0L;
          long lastFailure = 0L;

          try {
            while (!Thread.currentThread().isInterrupted()) {
              final Set<SegmentId> segmentsToRefresh = new TreeSet<>();
              final Set<String> dataSourcesToRebuild = new TreeSet<>();

              try {
                synchronized (lock) {
                  final long nextRefreshNoFuzz = DateTimes
                      .utc(lastRefresh)
                      .plus(config.getMetadataRefreshPeriod())
                      .getMillis();

                  // Fuzz a bit to spread load out when we have multiple brokers.
                  final long nextRefresh = nextRefreshNoFuzz + (long) ((nextRefreshNoFuzz - lastRefresh) * 0.10);

                  while (true) {
                    // Do not refresh if it's too soon after a failure (to avoid rapid cycles of failure).
                    final boolean wasRecentFailure = DateTimes.utc(lastFailure)
                                                              .plus(config.getMetadataRefreshPeriod())
                                                              .isAfterNow();

                    if (isServerViewInitialized &&
                        !wasRecentFailure &&
                        (!segmentsNeedingRefresh.isEmpty() || !dataSourcesNeedingRebuild.isEmpty()) &&
                        (refreshImmediately || nextRefresh < System.currentTimeMillis())) {
                      // We need to do a refresh. Break out of the waiting loop.
                      break;
                    }

                    // lastFailure != 0L means exceptions happened before and there're some refresh work was not completed.
                    // so that even if ServerView is initialized, we can't let broker complete initialization.
                    if (isServerViewInitialized && lastFailure == 0L) {
                      // Server view is initialized, but we don't need to do a refresh. Could happen if there are
                      // no segments in the system yet. Just mark us as initialized, then.
                      initialized.countDown();
                    }

                    // Wait some more, we'll wake up when it might be time to do another refresh.
                    lock.wait(Math.max(1, nextRefresh - System.currentTimeMillis()));
                  }

                  segmentsToRefresh.addAll(segmentsNeedingRefresh);
                  segmentsNeedingRefresh.clear();

                  // Mutable segments need a refresh every period, since new columns could be added dynamically.
                  segmentsNeedingRefresh.addAll(mutableSegments);

                  lastFailure = 0L;
                  lastRefresh = System.currentTimeMillis();
                  refreshImmediately = false;
                }

                refresh(segmentsToRefresh, dataSourcesToRebuild);

                initialized.countDown();
              }
              catch (InterruptedException e) {
                // Fall through.
                throw e;
              }
              catch (Exception e) {
                log.warn(e, "Metadata refresh failed, trying again soon.");

                synchronized (lock) {
                  // Add our segments and dataSources back to their refresh and rebuild lists.
                  segmentsNeedingRefresh.addAll(segmentsToRefresh);
                  dataSourcesNeedingRebuild.addAll(dataSourcesToRebuild);
                  lastFailure = System.currentTimeMillis();
                }
              }
            }
          }
          catch (InterruptedException e) {
            // Just exit.
          }
          catch (Throwable e) {
            // Throwables that fall out to here (not caught by an inner try/catch) are potentially gnarly, like
            // OOMEs. Anyway, let's just emit an alert and stop refreshing metadata.
            log.makeAlert(e, "Metadata refresh failed permanently").emit();
            throw e;
          }
          finally {
            log.info("Metadata refresh stopped.");
          }
        }
    );
  }

  @LifecycleStart
  public void start() throws InterruptedException
  {
    startCacheExec();

    if (config.isAwaitInitializationOnStart()) {
      final long startNanos = System.nanoTime();
      log.debug("%s waiting for initialization.", getClass().getSimpleName());
      awaitInitialization();
      log.info("%s initialized in [%,d] ms.", getClass().getSimpleName(), (System.nanoTime() - startNanos) / 1000000);
    }
  }

  @VisibleForTesting
  void refresh(final Set<SegmentId> segmentsToRefresh, final Set<String> dataSourcesToRebuild) throws IOException
  {
    // Refresh the segments.
    final Set<SegmentId> refreshed = refreshSegments(segmentsToRefresh);

    synchronized (lock) {
      // Add missing segments back to the refresh list.
      segmentsNeedingRefresh.addAll(Sets.difference(segmentsToRefresh, refreshed));

      // Compute the list of dataSources to rebuild tables for.
      dataSourcesToRebuild.addAll(dataSourcesNeedingRebuild);
      refreshed.forEach(segment -> dataSourcesToRebuild.add(segment.getDataSource()));
      dataSourcesNeedingRebuild.clear();
    }

    // Rebuild the dataSources.
    for (String dataSource : dataSourcesToRebuild) {
      final DatasourceTable.PhysicalDatasourceMetadata druidTable = buildDruidTable(dataSource);
      if (druidTable == null) {
        log.info("dataSource [%s] no longer exists, all metadata removed.", dataSource);
        tables.remove(dataSource);
        continue;
      }
      final DatasourceTable.PhysicalDatasourceMetadata oldTable = tables.put(dataSource, druidTable);
      final String description = druidTable.dataSource().isGlobal() ? "global dataSource" : "dataSource";
      if (oldTable == null || !oldTable.rowSignature().equals(druidTable.rowSignature())) {
        log.info("%s [%s] has new signature: %s.", description, dataSource, druidTable.rowSignature());
      } else {
        log.debug("%s [%s] signature is unchanged.", description, dataSource);
      }
    }
  }

  @LifecycleStop
  public void stop()
  {
    cacheExec.shutdownNow();
    callbackExec.shutdownNow();
  }

  public void awaitInitialization() throws InterruptedException
  {
    initialized.await();
  }

  protected DatasourceTable.PhysicalDatasourceMetadata getDatasource(String name)
  {
    return tables.get(name);
  }

  protected Set<String> getDatasourceNames()
  {
    return tables.keySet();
  }

  @VisibleForTesting
  protected void addSegment(final DruidServerMetadata server, final DataSegment segment)
  {
    // Get lock first so that we won't wait in ConcurrentMap.compute().
    synchronized (lock) {
      // someday we could hypothetically remove broker special casing, whenever BrokerServerView supports tracking
      // broker served segments in the timeline, to ensure that removeSegment the event is triggered accurately
      if (server.getType().equals(ServerType.BROKER)) {
        // a segment on a broker means a broadcast datasource, skip metadata because we'll also see this segment on the
        // historical, however mark the datasource for refresh because it needs to be globalized
        markDataSourceAsNeedRebuild(segment.getDataSource());
      } else {
        segmentMetadataInfo.compute(
            segment.getDataSource(),
            (datasource, segmentsMap) -> {
              if (segmentsMap == null) {
                segmentsMap = new ConcurrentSkipListMap<>(SEGMENT_ORDER);
              }
              segmentsMap.compute(
                  segment.getId(),
                  (segmentId, segmentMetadata) -> {
                    if (segmentMetadata == null) {
                      // Unknown segment.
                      totalSegments++;
                      // segmentReplicatable is used to determine if segments are served by historical or realtime servers
                      long isRealtime = server.isSegmentReplicationTarget() ? 0 : 1;
                      segmentMetadata = AvailableSegmentMetadata
                          .builder(segment, isRealtime, ImmutableSet.of(server), null, DEFAULT_NUM_ROWS) // Added without needing a refresh
                          .build();
                      markSegmentAsNeedRefresh(segment.getId());
                      if (!server.isSegmentReplicationTarget()) {
                        log.debug("Added new mutable segment [%s].", segment.getId());
                        markSegmentAsMutable(segment.getId());
                      } else {
                        log.debug("Added new immutable segment [%s].", segment.getId());
                      }
                    } else {
                      // We know this segment.
                      final Set<DruidServerMetadata> segmentServers = segmentMetadata.getReplicas();
                      final ImmutableSet<DruidServerMetadata> servers = new ImmutableSet.Builder<DruidServerMetadata>()
                          .addAll(segmentServers)
                          .add(server)
                          .build();
                      segmentMetadata = AvailableSegmentMetadata
                          .from(segmentMetadata)
                          .withReplicas(servers)
                          .withRealtime(recomputeIsRealtime(servers))
                          .build();
                      if (server.isSegmentReplicationTarget()) {
                        // If a segment shows up on a replicatable (historical) server at any point, then it must be immutable,
                        // even if it's also available on non-replicatable (realtime) servers.
                        unmarkSegmentAsMutable(segment.getId());
                        log.debug("Segment[%s] has become immutable.", segment.getId());
                      }
                    }
                    assert segmentMetadata != null;
                    return segmentMetadata;
                  }
              );

              return segmentsMap;
            }
        );
      }
      if (!tables.containsKey(segment.getDataSource())) {
        refreshImmediately = true;
      }

      lock.notifyAll();
    }
  }

  @VisibleForTesting
  void removeSegment(final DataSegment segment)
  {
    // Get lock first so that we won't wait in ConcurrentMap.compute().
    synchronized (lock) {
      log.debug("Segment [%s] is gone.", segment.getId());

      segmentsNeedingRefresh.remove(segment.getId());
      unmarkSegmentAsMutable(segment.getId());

      segmentMetadataInfo.compute(
          segment.getDataSource(),
          (dataSource, segmentsMap) -> {
            if (segmentsMap == null) {
              log.warn("Unknown segment [%s] was removed from the cluster. Ignoring this event.", segment.getId());
              return null;
            } else {
              if (segmentsMap.remove(segment.getId()) == null) {
                log.warn("Unknown segment [%s] was removed from the cluster. Ignoring this event.", segment.getId());
              } else {
                totalSegments--;
              }
              if (segmentsMap.isEmpty()) {
                tables.remove(segment.getDataSource());
                log.info("dataSource [%s] no longer exists, all metadata removed.", segment.getDataSource());
                return null;
              } else {
                markDataSourceAsNeedRebuild(segment.getDataSource());
                return segmentsMap;
              }
            }
          }
      );

      lock.notifyAll();
    }
  }

  @VisibleForTesting
  void removeServerSegment(final DruidServerMetadata server, final DataSegment segment)
  {
    // Get lock first so that we won't wait in ConcurrentMap.compute().
    synchronized (lock) {
      log.debug("Segment [%s] is gone from server [%s]", segment.getId(), server.getName());
      segmentMetadataInfo.compute(
          segment.getDataSource(),
          (datasource, knownSegments) -> {
            if (knownSegments == null) {
              log.warn(
                  "Unknown segment [%s] is removed from server [%s]. Ignoring this event",
                  segment.getId(),
                  server.getHost()
              );
              return null;
            }

            if (server.getType().equals(ServerType.BROKER)) {
              // for brokers, if the segment drops from all historicals before the broker this could be null.
              if (!knownSegments.isEmpty()) {
                // a segment on a broker means a broadcast datasource, skip metadata because we'll also see this segment on the
                // historical, however mark the datasource for refresh because it might no longer be broadcast or something
                markDataSourceAsNeedRebuild(segment.getDataSource());
              }
            } else {
              knownSegments.compute(
                  segment.getId(),
                  (segmentId, segmentMetadata) -> {
                    if (segmentMetadata == null) {
                      log.warn(
                          "Unknown segment [%s] is removed from server [%s]. Ignoring this event",
                          segment.getId(),
                          server.getHost()
                      );
                      return null;
                    } else {
                      final Set<DruidServerMetadata> segmentServers = segmentMetadata.getReplicas();
                      final ImmutableSet<DruidServerMetadata> servers = FluentIterable
                          .from(segmentServers)
                          .filter(Predicates.not(Predicates.equalTo(server)))
                          .toSet();
                      return AvailableSegmentMetadata
                          .from(segmentMetadata)
                          .withReplicas(servers)
                          .withRealtime(recomputeIsRealtime(servers))
                          .build();
                    }
                  }
              );
            }
            if (knownSegments.isEmpty()) {
              return null;
            } else {
              return knownSegments;
            }
          }
      );

      lock.notifyAll();
    }
  }

  private void markSegmentAsNeedRefresh(SegmentId segmentId)
  {
    synchronized (lock) {
      segmentsNeedingRefresh.add(segmentId);
    }
  }

  private void markSegmentAsMutable(SegmentId segmentId)
  {
    synchronized (lock) {
      mutableSegments.add(segmentId);
    }
  }

  private void unmarkSegmentAsMutable(SegmentId segmentId)
  {
    synchronized (lock) {
      mutableSegments.remove(segmentId);
    }
  }

  @VisibleForTesting
  void markDataSourceAsNeedRebuild(String datasource)
  {
    synchronized (lock) {
      dataSourcesNeedingRebuild.add(datasource);
    }
  }

  /**
   * Attempt to refresh "segmentSignatures" for a set of segments. Returns the set of segments actually refreshed,
   * which may be a subset of the asked-for set.
   */
  @VisibleForTesting
  protected Set<SegmentId> refreshSegments(final Set<SegmentId> segments) throws IOException
  {
    final Set<SegmentId> retVal = new HashSet<>();

    // Organize segments by dataSource.
    final Map<String, TreeSet<SegmentId>> segmentMap = new TreeMap<>();

    for (SegmentId segmentId : segments) {
      segmentMap.computeIfAbsent(segmentId.getDataSource(), x -> new TreeSet<>(SEGMENT_ORDER))
                .add(segmentId);
    }

    for (Map.Entry<String, TreeSet<SegmentId>> entry : segmentMap.entrySet()) {
      final String dataSource = entry.getKey();
      retVal.addAll(refreshSegmentsForDataSource(dataSource, entry.getValue()));
    }

    return retVal;
  }

  private long recomputeIsRealtime(ImmutableSet<DruidServerMetadata> servers)
  {
    if (servers.isEmpty()) {
      return 0;
    }
    final Optional<DruidServerMetadata> historicalServer = servers
        .stream()
        // Ideally, this filter should have checked whether it's a broadcast segment loaded in brokers.
        // However, we don't current track of the broadcast segments loaded in brokers, so this filter is still valid.
        // See addSegment(), removeServerSegment(), and removeSegment()
        .filter(metadata -> metadata.getType().equals(ServerType.HISTORICAL))
        .findAny();

    // if there is any historical server in the replicas, isRealtime flag should be unset
    return historicalServer.isPresent() ? 0 : 1;
  }

  /**
   * Attempt to refresh "segmentSignatures" for a set of segments for a particular dataSource. Returns the set of
   * segments actually refreshed, which may be a subset of the asked-for set.
   */
  private Set<SegmentId> refreshSegmentsForDataSource(final String dataSource, final Set<SegmentId> segments)
      throws IOException
  {
    if (!segments.stream().allMatch(segmentId -> segmentId.getDataSource().equals(dataSource))) {
      // Sanity check. We definitely expect this to pass.
      throw new ISE("'segments' must all match 'dataSource'!");
    }

    log.debug("Refreshing metadata for dataSource[%s].", dataSource);

    final long startTime = System.currentTimeMillis();

    // Segment id string -> SegmentId object.
    final Map<String, SegmentId> segmentIdMap = Maps.uniqueIndex(segments, SegmentId::toString);

    final Set<SegmentId> retVal = new HashSet<>();
    final Sequence<SegmentAnalysis> sequence = runSegmentMetadataQuery(
        Iterables.limit(segments, MAX_SEGMENTS_PER_QUERY)
    );

    Yielder<SegmentAnalysis> yielder = Yielders.each(sequence);

    try {
      while (!yielder.isDone()) {
        final SegmentAnalysis analysis = yielder.get();
        final SegmentId segmentId = segmentIdMap.get(analysis.getId());

        if (segmentId == null) {
          log.warn("Got analysis for segment [%s] we didn't ask for, ignoring.", analysis.getId());
        } else {
          final RowSignature rowSignature = analysisToRowSignature(analysis);
          log.debug("Segment[%s] has signature[%s].", segmentId, rowSignature);
          segmentMetadataInfo.compute(
              dataSource,
              (datasourceKey, dataSourceSegments) -> {
                if (dataSourceSegments == null) {
                  // Datasource may have been removed or become unavailable while this refresh was ongoing.
                  log.warn(
                      "No segment map found with datasource [%s], skipping refresh of segment [%s]",
                      datasourceKey,
                      segmentId
                  );
                  return null;
                } else {
                  dataSourceSegments.compute(
                      segmentId,
                      (segmentIdKey, segmentMetadata) -> {
                        if (segmentMetadata == null) {
                          log.warn("No segment [%s] found, skipping refresh", segmentId);
                          return null;
                        } else {
                          final AvailableSegmentMetadata updatedSegmentMetadata = AvailableSegmentMetadata
                              .from(segmentMetadata)
                              .withRowSignature(rowSignature)
                              .withNumRows(analysis.getNumRows())
                              .build();
                          retVal.add(segmentId);
                          return updatedSegmentMetadata;
                        }
                      }
                  );

                  if (dataSourceSegments.isEmpty()) {
                    return null;
                  } else {
                    return dataSourceSegments;
                  }
                }
              }
          );
        }

        yielder = yielder.next(null);
      }
    }
    finally {
      yielder.close();
    }

    log.debug(
        "Refreshed metadata for dataSource [%s] in %,d ms (%d segments queried, %d segments left).",
        dataSource,
        System.currentTimeMillis() - startTime,
        retVal.size(),
        segments.size() - retVal.size()
    );

    return retVal;
  }

  @VisibleForTesting
  @Nullable
  DatasourceTable.PhysicalDatasourceMetadata buildDruidTable(final String dataSource)
  {
    ConcurrentSkipListMap<SegmentId, AvailableSegmentMetadata> segmentsMap = segmentMetadataInfo.get(dataSource);

    // Preserve order.
    final Map<String, ColumnType> columnTypes = new LinkedHashMap<>();

    if (segmentsMap != null && !segmentsMap.isEmpty()) {
      for (AvailableSegmentMetadata availableSegmentMetadata : segmentsMap.values()) {
        final RowSignature rowSignature = availableSegmentMetadata.getRowSignature();
        if (rowSignature != null) {
          for (String column : rowSignature.getColumnNames()) {
            // Newer column types should override older ones.
            final ColumnType columnType =
                rowSignature.getColumnType(column)
                            .orElseThrow(() -> new ISE("Encountered null type for column [%s]", column));

            columnTypes.putIfAbsent(column, columnType);
          }
        }
      }
    } else {
      // table has no segments
      return null;
    }

    final RowSignature.Builder builder = RowSignature.builder();
    columnTypes.forEach(builder::add);

    final TableDataSource tableDataSource;

    // to be a GlobalTableDataSource instead of a TableDataSource, it must appear on all servers (inferred by existing
    // in the segment cache, which in this case belongs to the broker meaning only broadcast segments live here)
    // to be joinable, it must be possibly joinable according to the factory. we only consider broadcast datasources
    // at this time, and isGlobal is currently strongly coupled with joinable, so only make a global table datasource
    // if also joinable
    final GlobalTableDataSource maybeGlobal = new GlobalTableDataSource(dataSource);
    final boolean isJoinable = joinableFactory.isDirectlyJoinable(maybeGlobal);
    final boolean isBroadcast = segmentManager.getDataSourceNames().contains(dataSource);
    if (isBroadcast && isJoinable) {
      tableDataSource = maybeGlobal;
    } else {
      tableDataSource = new TableDataSource(dataSource);
    }
    return new DatasourceTable.PhysicalDatasourceMetadata(tableDataSource, builder.build(), isJoinable, isBroadcast);
  }

  @VisibleForTesting
  Map<SegmentId, AvailableSegmentMetadata> getSegmentMetadataSnapshot()
  {
    final Map<SegmentId, AvailableSegmentMetadata> segmentMetadata = Maps.newHashMapWithExpectedSize(totalSegments);
    for (ConcurrentSkipListMap<SegmentId, AvailableSegmentMetadata> val : segmentMetadataInfo.values()) {
      segmentMetadata.putAll(val);
    }
    return segmentMetadata;
  }

  /**
   * Returns total number of segments. This method doesn't use the lock intentionally to avoid expensive contention.
   * As a result, the returned value might be inexact.
   */
  int getTotalSegments()
  {
    return totalSegments;
  }

  @VisibleForTesting
  TreeSet<SegmentId> getSegmentsNeedingRefresh()
  {
    synchronized (lock) {
      return segmentsNeedingRefresh;
    }
  }

  @VisibleForTesting
  TreeSet<SegmentId> getMutableSegments()
  {
    synchronized (lock) {
      return mutableSegments;
    }
  }

  @VisibleForTesting
  Set<String> getDataSourcesNeedingRebuild()
  {
    synchronized (lock) {
      return dataSourcesNeedingRebuild;
    }
  }

  /**
   * Execute a SegmentMetadata query and return a {@link Sequence} of {@link SegmentAnalysis}.
   *
   * @param segments Iterable of {@link SegmentId} objects that are subject of the SegmentMetadata query.
   * @return {@link Sequence} of {@link SegmentAnalysis} objects
   */
  @VisibleForTesting
  protected Sequence<SegmentAnalysis> runSegmentMetadataQuery(
      final Iterable<SegmentId> segments
  )
  {
    // Sanity check: getOnlyElement of a set, to ensure all segments have the same dataSource.
    final String dataSource = Iterables.getOnlyElement(
        StreamSupport.stream(segments.spliterator(), false)
                     .map(SegmentId::getDataSource).collect(Collectors.toSet())
    );

    final MultipleSpecificSegmentSpec querySegmentSpec = new MultipleSpecificSegmentSpec(
        StreamSupport.stream(segments.spliterator(), false)
                     .map(SegmentId::toDescriptor).collect(Collectors.toList())
    );

    final SegmentMetadataQuery segmentMetadataQuery = new SegmentMetadataQuery(
        new TableDataSource(dataSource),
        querySegmentSpec,
        new AllColumnIncluderator(),
        false,
        brokerInternalQueryConfig.getContext(),
        EnumSet.noneOf(SegmentMetadataQuery.AnalysisType.class),
        false,
        false
    );

    return queryLifecycleFactory
        .factorize()
        .runSimple(segmentMetadataQuery, escalator.createEscalatedAuthenticationResult(), Access.OK).getResults();
  }

  @VisibleForTesting
  static RowSignature analysisToRowSignature(final SegmentAnalysis analysis)
  {
    final RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    for (Map.Entry<String, ColumnAnalysis> entry : analysis.getColumns().entrySet()) {
      if (entry.getValue().isError()) {
        // Skip columns with analysis errors.
        continue;
      }

      ColumnType valueType = entry.getValue().getTypeSignature();

      // this shouldn't happen, but if it does, first try to fall back to legacy type information field in case
      // standard upgrade order was not followed for 0.22 to 0.23+, and if that also fails, then assume types are some
      // flavor of COMPLEX.
      if (valueType == null) {
        // at some point in the future this can be simplified to the contents of the catch clause here, once the
        // likelyhood of upgrading from some version lower than 0.23 is low
        try {
          valueType = ColumnType.fromString(entry.getValue().getType());
        }
        catch (IllegalArgumentException ignored) {
          valueType = ColumnType.UNKNOWN_COMPLEX;
        }
      }

      rowSignatureBuilder.add(entry.getKey(), valueType);
    }
    return ROW_SIGNATURE_INTERNER.intern(rowSignatureBuilder.build());
  }

  /**
   * This method is not thread-safe and must be used only in unit tests.
   */
  @VisibleForTesting
  void setAvailableSegmentMetadata(final SegmentId segmentId, final AvailableSegmentMetadata availableSegmentMetadata)
  {
    final ConcurrentSkipListMap<SegmentId, AvailableSegmentMetadata> dataSourceSegments = segmentMetadataInfo
        .computeIfAbsent(
            segmentId.getDataSource(),
            k -> new ConcurrentSkipListMap<>(SEGMENT_ORDER)
        );
    if (dataSourceSegments.put(segmentId, availableSegmentMetadata) == null) {
      totalSegments++;
    }
  }

  /**
   * This is a helper method for unit tests to emulate heavy work done with {@link #lock}.
   * It must be used only in unit tests.
   */
  @VisibleForTesting
  void doInLock(Runnable runnable)
  {
    synchronized (lock) {
      runnable.run();
    }
  }
}
