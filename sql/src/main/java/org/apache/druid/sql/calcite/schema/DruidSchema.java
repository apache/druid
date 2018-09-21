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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.druid.client.ServerView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.metadata.metadata.AllColumnIncluderator;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.RowSignature;
import org.apache.druid.sql.calcite.view.DruidViewMacro;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.apache.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
  private static final Comparator<DataSegment> SEGMENT_ORDER = Comparator
      .comparing((DataSegment segment) -> segment.getInterval().getStart()).reversed()
      .thenComparing(Function.identity());

  public static final String NAME = "druid";

  private static final EmittingLogger log = new EmittingLogger(DruidSchema.class);
  private static final int MAX_SEGMENTS_PER_QUERY = 15000;

  private final QueryLifecycleFactory queryLifecycleFactory;
  private final TimelineServerView serverView;
  private final PlannerConfig config;
  private final ViewManager viewManager;
  private final ExecutorService cacheExec;
  private final ConcurrentMap<String, DruidTable> tables;

  // For awaitInitialization.
  private final CountDownLatch initializationLatch = new CountDownLatch(1);

  // Protects access to segmentSignatures, mutableSegments, segmentsNeedingRefresh, lastRefresh, isServerViewInitialized
  private final Object lock = new Object();

  // DataSource -> Segment -> RowSignature for that segment.
  // Use TreeMap for segments so they are merged in deterministic order, from older to newer.
  private final Map<String, TreeMap<DataSegment, RowSignature>> segmentSignatures = new HashMap<>();

  // All mutable segments.
  private final Set<DataSegment> mutableSegments = new TreeSet<>(SEGMENT_ORDER);

  // All dataSources that need tables regenerated.
  private final Set<String> dataSourcesNeedingRebuild = new HashSet<>();

  // All segments that need to be refreshed.
  private final TreeSet<DataSegment> segmentsNeedingRefresh = new TreeSet<>(SEGMENT_ORDER);

  // Escalator, so we can attach an authentication result to queries we generate.
  private final Escalator escalator;

  private boolean refreshImmediately = false;
  private long lastRefresh = 0L;
  private long lastFailure = 0L;
  private boolean isServerViewInitialized = false;

  @Inject
  public DruidSchema(
      final QueryLifecycleFactory queryLifecycleFactory,
      final TimelineServerView serverView,
      final PlannerConfig config,
      final ViewManager viewManager,
      final Escalator escalator
  )
  {
    this.queryLifecycleFactory = Preconditions.checkNotNull(queryLifecycleFactory, "queryLifecycleFactory");
    this.serverView = Preconditions.checkNotNull(serverView, "serverView");
    this.config = Preconditions.checkNotNull(config, "config");
    this.viewManager = Preconditions.checkNotNull(viewManager, "viewManager");
    this.cacheExec = ScheduledExecutors.fixed(1, "DruidSchema-Cache-%d");
    this.tables = new ConcurrentHashMap<>();
    this.escalator = escalator;

    serverView.registerTimelineCallback(
        MoreExecutors.sameThreadExecutor(),
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
        }
    );
  }

  @LifecycleStart
  public void start()
  {
    cacheExec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              while (!Thread.currentThread().isInterrupted()) {
                final Set<DataSegment> segmentsToRefresh = new TreeSet<>();
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
                        break;
                      }
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
                  final Set<DataSegment> refreshed = refreshSegments(segmentsToRefresh);

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
                    if (oldTable == null || !oldTable.getRowSignature().equals(druidTable.getRowSignature())) {
                      log.debug(
                          "Table for dataSource[%s] has new signature[%s].",
                          dataSource,
                          druidTable.getRowSignature()
                      );
                    } else {
                      log.debug("Table for dataSource[%s] signature is unchanged.", dataSource);
                    }
                  }

                  initializationLatch.countDown();
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
        }
    );
  }

  @LifecycleStop
  public void stop()
  {
    cacheExec.shutdownNow();
  }

  @VisibleForTesting
  public void awaitInitialization() throws InterruptedException
  {
    initializationLatch.await();
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

  private void addSegment(final DruidServerMetadata server, final DataSegment segment)
  {
    synchronized (lock) {
      final Map<DataSegment, RowSignature> knownSegments = segmentSignatures.get(segment.getDataSource());
      if (knownSegments == null || !knownSegments.containsKey(segment)) {
        // Unknown segment.
        setSegmentSignature(segment, null);
        segmentsNeedingRefresh.add(segment);

        if (!server.segmentReplicatable()) {
          log.debug("Added new mutable segment[%s].", segment.getId());
          mutableSegments.add(segment);
        } else {
          log.debug("Added new immutable segment[%s].", segment.getId());
        }
      } else if (server.segmentReplicatable()) {
        // If a segment shows up on a replicatable (historical) server at any point, then it must be immutable,
        // even if it's also available on non-replicatable (realtime) servers.
        mutableSegments.remove(segment);
        log.debug("Segment[%s] has become immutable.", segment.getId());
      }

      if (!tables.containsKey(segment.getDataSource())) {
        refreshImmediately = true;
      }

      lock.notifyAll();
    }
  }

  private void removeSegment(final DataSegment segment)
  {
    synchronized (lock) {
      log.debug("Segment[%s] is gone.", segment.getId());

      dataSourcesNeedingRebuild.add(segment.getDataSource());
      segmentsNeedingRefresh.remove(segment);
      mutableSegments.remove(segment);

      final Map<DataSegment, RowSignature> dataSourceSegments = segmentSignatures.get(segment.getDataSource());
      dataSourceSegments.remove(segment);

      if (dataSourceSegments.isEmpty()) {
        segmentSignatures.remove(segment.getDataSource());
        tables.remove(segment.getDataSource());
        log.info("Removed all metadata for dataSource[%s].", segment.getDataSource());
      }

      lock.notifyAll();
    }
  }

  /**
   * Attempt to refresh "segmentSignatures" for a set of segments. Returns the set of segments actually refreshed,
   * which may be a subset of the asked-for set.
   */
  private Set<DataSegment> refreshSegments(final Set<DataSegment> segments) throws IOException
  {
    final Set<DataSegment> retVal = new HashSet<>();

    // Organize segments by dataSource.
    final Map<String, TreeSet<DataSegment>> segmentMap = new TreeMap<>();

    for (DataSegment segment : segments) {
      segmentMap.computeIfAbsent(segment.getDataSource(), x -> new TreeSet<>(SEGMENT_ORDER))
                .add(segment);
    }

    for (Map.Entry<String, TreeSet<DataSegment>> entry : segmentMap.entrySet()) {
      final String dataSource = entry.getKey();
      retVal.addAll(refreshSegmentsForDataSource(dataSource, entry.getValue()));
    }

    return retVal;
  }

  /**
   * Attempt to refresh "segmentSignatures" for a set of segments for a particular dataSource. Returns the set of
   * segments actually refreshed, which may be a subset of the asked-for set.
   */
  private Set<DataSegment> refreshSegmentsForDataSource(final String dataSource, final Set<DataSegment> segments)
      throws IOException
  {
    log.debug("Refreshing metadata for dataSource[%s].", dataSource);

    final long startTime = System.currentTimeMillis();

    // Segment id -> segment object.
    final Map<String, DataSegment> segmentMap = Maps.uniqueIndex(segments, segment -> segment.getId().toString());

    final Set<DataSegment> retVal = new HashSet<>();
    final Sequence<SegmentAnalysis> sequence = runSegmentMetadataQuery(
        queryLifecycleFactory,
        Iterables.limit(segments, MAX_SEGMENTS_PER_QUERY),
        escalator.createEscalatedAuthenticationResult()
    );

    Yielder<SegmentAnalysis> yielder = Yielders.each(sequence);

    try {
      while (!yielder.isDone()) {
        final SegmentAnalysis analysis = yielder.get();
        final DataSegment segment = segmentMap.get(analysis.getId());

        if (segment == null) {
          log.warn("Got analysis for segment[%s] we didn't ask for, ignoring.", analysis.getId());
        } else {
          final RowSignature rowSignature = analysisToRowSignature(analysis);
          log.debug("Segment[%s] has signature[%s].", segment.getId(), rowSignature);
          setSegmentSignature(segment, rowSignature);
          retVal.add(segment);
        }

        yielder = yielder.next(null);
      }
    }
    finally {
      yielder.close();
    }

    log.info(
        "Refreshed metadata for dataSource[%s] in %,d ms (%d segments queried, %d segments left).",
        dataSource,
        System.currentTimeMillis() - startTime,
        retVal.size(),
        segments.size() - retVal.size()
    );

    return retVal;
  }

  private void setSegmentSignature(final DataSegment segment, final RowSignature rowSignature)
  {
    synchronized (lock) {
      segmentSignatures.computeIfAbsent(segment.getDataSource(), x -> new TreeMap<>(SEGMENT_ORDER))
                       .put(segment, rowSignature);
    }
  }

  private DruidTable buildDruidTable(final String dataSource)
  {
    synchronized (lock) {
      final TreeMap<DataSegment, RowSignature> segmentMap = segmentSignatures.get(dataSource);
      final Map<String, ValueType> columnTypes = new TreeMap<>();

      if (segmentMap != null) {
        for (RowSignature rowSignature : segmentMap.values()) {
          if (rowSignature != null) {
            for (String column : rowSignature.getRowOrder()) {
              // Newer column types should override older ones.
              columnTypes.putIfAbsent(column, rowSignature.getColumnType(column));
            }
          }
        }
      }

      final RowSignature.Builder builder = RowSignature.builder();
      columnTypes.forEach(builder::add);
      return new DruidTable(new TableDataSource(dataSource), builder.build());
    }
  }

  private static Sequence<SegmentAnalysis> runSegmentMetadataQuery(
      final QueryLifecycleFactory queryLifecycleFactory,
      final Iterable<DataSegment> segments,
      final AuthenticationResult authenticationResult
  )
  {
    // Sanity check: getOnlyElement of a set, to ensure all segments have the same dataSource.
    final String dataSource = Iterables.getOnlyElement(
        StreamSupport.stream(segments.spliterator(), false)
                     .map(DataSegment::getDataSource).collect(Collectors.toSet())
    );

    final MultipleSpecificSegmentSpec querySegmentSpec = new MultipleSpecificSegmentSpec(
        StreamSupport.stream(segments.spliterator(), false)
                     .map(DataSegment::toDescriptor).collect(Collectors.toList())
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
}
