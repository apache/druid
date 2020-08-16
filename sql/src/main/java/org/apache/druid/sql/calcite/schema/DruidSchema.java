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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.druid.client.ServerView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
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
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.view.DruidViewMacro;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.io.IOException;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@ManageLifecycle
public class DruidSchema extends AbstractSchema
{
  // Newest segments first, so they override older ones.
  private static final Comparator<SegmentId> SEGMENT_ORDER = Comparator
      .comparing((SegmentId segmentId) -> segmentId.getInterval().getStart())
      .reversed()
      .thenComparing(Function.identity());

  private static final EmittingLogger log = new EmittingLogger(DruidSchema.class);
  private static final int MAX_SEGMENTS_PER_QUERY = 15000;
  private static final long DEFAULT_NUM_ROWS = 0;

  private final QueryLifecycleFactory queryLifecycleFactory;
  private final PlannerConfig config;
  private final SegmentManager segmentManager;
  private final ViewManager viewManager;
  private final JoinableFactory joinableFactory;
  private final ExecutorService cacheExec;
  private final ConcurrentMap<String, DruidTable> tables;

  // For awaitInitialization.
  private final CountDownLatch initialized = new CountDownLatch(1);

  // Protects access to segmentSignatures, mutableSegments, segmentsNeedingRefresh, lastRefresh, isServerViewInitialized, segmentMetadata
  private final Object lock = new Object();

  // DataSource -> Segment -> AvailableSegmentMetadata(contains RowSignature) for that segment.
  // Use TreeMap for segments so they are merged in deterministic order, from older to newer.
  @GuardedBy("lock")
  private final Map<String, TreeMap<SegmentId, AvailableSegmentMetadata>> segmentMetadataInfo = new HashMap<>();
  private int totalSegments = 0;

  // All mutable segments.
  @GuardedBy("lock")
  private final Set<SegmentId> mutableSegments = new TreeSet<>(SEGMENT_ORDER);

  // All dataSources that need tables regenerated.
  @GuardedBy("lock")
  private final Set<String> dataSourcesNeedingRebuild = new HashSet<>();

  // All segments that need to be refreshed.
  @GuardedBy("lock")
  private final TreeSet<SegmentId> segmentsNeedingRefresh = new TreeSet<>(SEGMENT_ORDER);

  // Escalator, so we can attach an authentication result to queries we generate.
  private final Escalator escalator;

  @GuardedBy("lock")
  private boolean refreshImmediately = false;
  @GuardedBy("lock")
  private long lastRefresh = 0L;
  @GuardedBy("lock")
  private long lastFailure = 0L;
  @GuardedBy("lock")
  private boolean isServerViewInitialized = false;

  @Inject
  public DruidSchema(
      final QueryLifecycleFactory queryLifecycleFactory,
      final TimelineServerView serverView,
      final SegmentManager segmentManager,
      final JoinableFactory joinableFactory,
      final PlannerConfig config,
      final ViewManager viewManager,
      final Escalator escalator
  )
  {
    this.queryLifecycleFactory = Preconditions.checkNotNull(queryLifecycleFactory, "queryLifecycleFactory");
    Preconditions.checkNotNull(serverView, "serverView");
    this.segmentManager = segmentManager;
    this.joinableFactory = joinableFactory;
    this.config = Preconditions.checkNotNull(config, "config");
    this.viewManager = Preconditions.checkNotNull(viewManager, "viewManager");
    this.cacheExec = Execs.singleThreaded("DruidSchema-Cache-%d");
    this.tables = new ConcurrentHashMap<>();
    this.escalator = escalator;

    serverView.registerTimelineCallback(
        Execs.directExecutor(),
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

  @LifecycleStart
  public void start() throws InterruptedException
  {
    cacheExec.submit(
        () -> {
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

                    if (isServerViewInitialized) {
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

                // Refresh the segments.
                final Set<SegmentId> refreshed = refreshSegments(segmentsToRefresh);

                synchronized (lock) {
                  // Add missing segments back to the refresh list.
                  segmentsNeedingRefresh.addAll(Sets.difference(segmentsToRefresh, refreshed));

                  // Compute the list of dataSources to rebuild tables for.
                  dataSourcesToRebuild.addAll(dataSourcesNeedingRebuild);
                  refreshed.forEach(segment -> dataSourcesToRebuild.add(segment.getDataSource()));
                  dataSourcesNeedingRebuild.clear();

                  lock.notifyAll();
                }

                // Rebuild the dataSources.
                for (String dataSource : dataSourcesToRebuild) {
                  final DruidTable druidTable = buildDruidTable(dataSource);
                  final DruidTable oldTable = tables.put(dataSource, druidTable);
                  final String description = druidTable.getDataSource().isGlobal() ? "global dataSource" : "dataSource";
                  if (oldTable == null || !oldTable.getRowSignature().equals(druidTable.getRowSignature())) {
                    log.info("%s [%s] has new signature: %s.", description, dataSource, druidTable.getRowSignature());
                  } else {
                    log.debug("%s [%s] signature is unchanged.", description, dataSource);
                  }
                }

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
                  lock.notifyAll();
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

    if (config.isAwaitInitializationOnStart()) {
      final long startNanos = System.nanoTime();
      log.debug("%s waiting for initialization.", getClass().getSimpleName());
      awaitInitialization();
      log.info("%s initialized in [%,d] ms.", getClass().getSimpleName(), (System.nanoTime() - startNanos) / 1000000);
    }
  }

  @LifecycleStop
  public void stop()
  {
    cacheExec.shutdownNow();
  }

  public void awaitInitialization() throws InterruptedException
  {
    initialized.await();
  }

  @Override
  protected Map<String, Table> getTableMap()
  {
    return ImmutableMap.copyOf(tables);
  }

  @Override
  protected Multimap<String, org.apache.calcite.schema.Function> getFunctionMultimap()
  {
    final ImmutableMultimap.Builder<String, org.apache.calcite.schema.Function> builder = ImmutableMultimap.builder();
    for (Map.Entry<String, DruidViewMacro> entry : viewManager.getViews().entrySet()) {
      builder.put(entry);
    }
    return builder.build();
  }

  @VisibleForTesting
  void addSegment(final DruidServerMetadata server, final DataSegment segment)
  {
    synchronized (lock) {
      if (server.getType().equals(ServerType.BROKER)) {
        // a segment on a broker means a broadcast datasource, skip metadata because we'll also see this segment on the
        // historical, however mark the datasource for refresh because it needs to be globalized
        dataSourcesNeedingRebuild.add(segment.getDataSource());
      } else {
        final Map<SegmentId, AvailableSegmentMetadata> knownSegments = segmentMetadataInfo.get(segment.getDataSource());
        AvailableSegmentMetadata segmentMetadata = knownSegments != null ? knownSegments.get(segment.getId()) : null;
        if (segmentMetadata == null) {
          // segmentReplicatable is used to determine if segments are served by historical or realtime servers
          long isRealtime = server.isSegmentReplicationTarget() ? 0 : 1;
          segmentMetadata = AvailableSegmentMetadata.builder(
              segment,
              isRealtime,
              ImmutableSet.of(server),
              null,
              DEFAULT_NUM_ROWS
          ).build();
          // Unknown segment.
          setAvailableSegmentMetadata(segment.getId(), segmentMetadata);
          segmentsNeedingRefresh.add(segment.getId());
          if (!server.isSegmentReplicationTarget()) {
            log.debug("Added new mutable segment[%s].", segment.getId());
            mutableSegments.add(segment.getId());
          } else {
            log.debug("Added new immutable segment[%s].", segment.getId());
          }
        } else {
          final Set<DruidServerMetadata> segmentServers = segmentMetadata.getReplicas();
          final ImmutableSet<DruidServerMetadata> servers = new ImmutableSet.Builder<DruidServerMetadata>()
              .addAll(segmentServers)
              .add(server)
              .build();
          final AvailableSegmentMetadata metadataWithNumReplicas = AvailableSegmentMetadata
              .from(segmentMetadata)
              .withReplicas(servers)
              .withRealtime(recomputeIsRealtime(servers))
              .build();
          knownSegments.put(segment.getId(), metadataWithNumReplicas);
          if (server.isSegmentReplicationTarget()) {
            // If a segment shows up on a replicatable (historical) server at any point, then it must be immutable,
            // even if it's also available on non-replicatable (realtime) servers.
            mutableSegments.remove(segment.getId());
            log.debug("Segment[%s] has become immutable.", segment.getId());
          }
        }
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
    synchronized (lock) {
      log.debug("Segment[%s] is gone.", segment.getId());

      dataSourcesNeedingRebuild.add(segment.getDataSource());
      segmentsNeedingRefresh.remove(segment.getId());
      mutableSegments.remove(segment.getId());

      final Map<SegmentId, AvailableSegmentMetadata> dataSourceSegments =
          segmentMetadataInfo.get(segment.getDataSource());
      if (dataSourceSegments.remove(segment.getId()) != null) {
        totalSegments--;
      }

      if (dataSourceSegments.isEmpty()) {
        segmentMetadataInfo.remove(segment.getDataSource());
        tables.remove(segment.getDataSource());
        log.info("dataSource[%s] no longer exists, all metadata removed.", segment.getDataSource());
      }

      lock.notifyAll();
    }
  }

  @VisibleForTesting
  void removeServerSegment(final DruidServerMetadata server, final DataSegment segment)
  {
    synchronized (lock) {
      log.debug("Segment[%s] is gone from server[%s]", segment.getId(), server.getName());
      if (server.getType().equals(ServerType.BROKER)) {
        // a segment on a broker means a broadcast datasource, skip metadata because we'll also see this segment on the
        // historical, however mark the datasource for refresh because it might no longer be broadcast or something
        dataSourcesNeedingRebuild.add(segment.getDataSource());
      } else {
        final Map<SegmentId, AvailableSegmentMetadata> knownSegments = segmentMetadataInfo.get(segment.getDataSource());
        final AvailableSegmentMetadata segmentMetadata = knownSegments.get(segment.getId());
        final Set<DruidServerMetadata> segmentServers = segmentMetadata.getReplicas();
        final ImmutableSet<DruidServerMetadata> servers = FluentIterable
            .from(segmentServers)
            .filter(Predicates.not(Predicates.equalTo(server)))
            .toSet();

        final AvailableSegmentMetadata metadataWithNumReplicas = AvailableSegmentMetadata
            .from(segmentMetadata)
            .withReplicas(servers)
            .withRealtime(recomputeIsRealtime(servers))
            .build();
        knownSegments.put(segment.getId(), metadataWithNumReplicas);
      }
      lock.notifyAll();
    }
  }

  /**
   * Attempt to refresh "segmentSignatures" for a set of segments. Returns the set of segments actually refreshed,
   * which may be a subset of the asked-for set.
   */
  @VisibleForTesting
  Set<SegmentId> refreshSegments(final Set<SegmentId> segments) throws IOException
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
    final Optional<DruidServerMetadata> historicalServer = servers
        .stream()
        .filter(metadata -> metadata.getType().equals(ServerType.HISTORICAL))
        .findAny();

    // if there is any historical server in the replicas, isRealtime flag should be unset
    final long isRealtime = historicalServer.isPresent() ? 0 : 1;
    return isRealtime;
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
        queryLifecycleFactory,
        Iterables.limit(segments, MAX_SEGMENTS_PER_QUERY),
        escalator.createEscalatedAuthenticationResult()
    );

    Yielder<SegmentAnalysis> yielder = Yielders.each(sequence);

    try {
      while (!yielder.isDone()) {
        final SegmentAnalysis analysis = yielder.get();
        final SegmentId segmentId = segmentIdMap.get(analysis.getId());

        if (segmentId == null) {
          log.warn("Got analysis for segment[%s] we didn't ask for, ignoring.", analysis.getId());
        } else {
          synchronized (lock) {
            final RowSignature rowSignature = analysisToRowSignature(analysis);
            log.debug("Segment[%s] has signature[%s].", segmentId, rowSignature);
            final Map<SegmentId, AvailableSegmentMetadata> dataSourceSegments = segmentMetadataInfo.get(dataSource);
            if (dataSourceSegments == null) {
              // Datasource may have been removed or become unavailable while this refresh was ongoing.
              log.warn(
                  "No segment map found with datasource[%s], skipping refresh of segment[%s]",
                  dataSource,
                  segmentId
              );
            } else {
              final AvailableSegmentMetadata segmentMetadata = dataSourceSegments.get(segmentId);
              if (segmentMetadata == null) {
                log.warn("No segment[%s] found, skipping refresh", segmentId);
              } else {
                final AvailableSegmentMetadata updatedSegmentMetadata = AvailableSegmentMetadata
                    .from(segmentMetadata)
                    .withRowSignature(rowSignature)
                    .withNumRows(analysis.getNumRows())
                    .build();
                dataSourceSegments.put(segmentId, updatedSegmentMetadata);
                setAvailableSegmentMetadata(segmentId, updatedSegmentMetadata);
                retVal.add(segmentId);
              }
            }
          }
        }

        yielder = yielder.next(null);
      }
    }
    finally {
      yielder.close();
    }

    log.debug(
        "Refreshed metadata for dataSource[%s] in %,d ms (%d segments queried, %d segments left).",
        dataSource,
        System.currentTimeMillis() - startTime,
        retVal.size(),
        segments.size() - retVal.size()
    );

    return retVal;
  }

  @VisibleForTesting
  void setAvailableSegmentMetadata(final SegmentId segmentId, final AvailableSegmentMetadata availableSegmentMetadata)
  {
    synchronized (lock) {
      TreeMap<SegmentId, AvailableSegmentMetadata> dataSourceSegments = segmentMetadataInfo.computeIfAbsent(
          segmentId.getDataSource(),
          x -> new TreeMap<>(SEGMENT_ORDER)
      );
      if (dataSourceSegments.put(segmentId, availableSegmentMetadata) == null) {
        totalSegments++;
      }
    }
  }

  protected DruidTable buildDruidTable(final String dataSource)
  {
    synchronized (lock) {
      final Map<SegmentId, AvailableSegmentMetadata> segmentMap = segmentMetadataInfo.get(dataSource);
      final Map<String, ValueType> columnTypes = new TreeMap<>();

      if (segmentMap != null) {
        for (AvailableSegmentMetadata availableSegmentMetadata : segmentMap.values()) {
          final RowSignature rowSignature = availableSegmentMetadata.getRowSignature();
          if (rowSignature != null) {
            for (String column : rowSignature.getColumnNames()) {
              // Newer column types should override older ones.
              final ValueType columnType =
                  rowSignature.getColumnType(column)
                              .orElseThrow(() -> new ISE("Encountered null type for column[%s]", column));

              columnTypes.putIfAbsent(column, columnType);
            }
          }
        }
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
      return new DruidTable(tableDataSource, builder.build(), isJoinable, isBroadcast);
    }
  }

  private static Sequence<SegmentAnalysis> runSegmentMetadataQuery(
      final QueryLifecycleFactory queryLifecycleFactory,
      final Iterable<SegmentId> segments,
      final AuthenticationResult authenticationResult
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
        ImmutableMap.of(),
        EnumSet.noneOf(SegmentMetadataQuery.AnalysisType.class),
        false,
        false
    );

    return queryLifecycleFactory.factorize().runSimple(segmentMetadataQuery, authenticationResult, null);
  }

  private static RowSignature analysisToRowSignature(final SegmentAnalysis analysis)
  {
    final RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    for (Map.Entry<String, ColumnAnalysis> entry : analysis.getColumns().entrySet()) {
      if (entry.getValue().isError()) {
        // Skip columns with analysis errors.
        continue;
      }

      ValueType valueType;
      try {
        valueType = ValueType.valueOf(StringUtils.toUpperCase(entry.getValue().getType()));
      }
      catch (IllegalArgumentException e) {
        // Assume unrecognized types are some flavor of COMPLEX. This throws away information about exactly
        // what kind of complex column it is, which we may want to preserve some day.
        valueType = ValueType.COMPLEX;
      }

      rowSignatureBuilder.add(entry.getKey(), valueType);
    }
    return rowSignatureBuilder.build();
  }

  Map<SegmentId, AvailableSegmentMetadata> getSegmentMetadataSnapshot()
  {
    final Map<SegmentId, AvailableSegmentMetadata> segmentMetadata = new HashMap<>();
    synchronized (lock) {
      for (TreeMap<SegmentId, AvailableSegmentMetadata> val : segmentMetadataInfo.values()) {
        segmentMetadata.putAll(val);
      }
    }
    return segmentMetadata;
  }

  int getTotalSegments()
  {
    return totalSegments;
  }
}
