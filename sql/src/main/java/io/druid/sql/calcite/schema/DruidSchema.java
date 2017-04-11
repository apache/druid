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

package io.druid.sql.calcite.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.ServerView;
import io.druid.client.TimelineServerView;
import io.druid.common.utils.JodaUtils;
import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.TableDataSource;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.segment.column.ValueType;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.sql.calcite.table.RowSignature;
import io.druid.sql.calcite.view.DruidViewMacro;
import io.druid.sql.calcite.view.ViewManager;
import io.druid.timeline.DataSegment;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.joda.time.DateTime;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

@ManageLifecycle
public class DruidSchema extends AbstractSchema
{
  public static final String NAME = "druid";

  private static final EmittingLogger log = new EmittingLogger(DruidSchema.class);

  private final QuerySegmentWalker walker;
  private final TimelineServerView serverView;
  private final PlannerConfig config;
  private final ViewManager viewManager;
  private final ExecutorService cacheExec;
  private final ConcurrentMap<String, Table> tables;

  // For awaitInitialization.
  private final CountDownLatch initializationLatch = new CountDownLatch(1);

  // Protects access to dataSourcesNeedingRefresh, lastRefresh, isServerViewInitialized
  private final Object lock = new Object();

  // List of dataSources that need metadata refreshes.
  private final Set<String> dataSourcesNeedingRefresh = Sets.newHashSet();
  private boolean refreshImmediately = false;
  private long lastRefresh = 0L;
  private boolean isServerViewInitialized = false;

  @Inject
  public DruidSchema(
      final QuerySegmentWalker walker,
      final TimelineServerView serverView,
      final PlannerConfig config,
      final ViewManager viewManager
  )
  {
    this.walker = Preconditions.checkNotNull(walker, "walker");
    this.serverView = Preconditions.checkNotNull(serverView, "serverView");
    this.config = Preconditions.checkNotNull(config, "config");
    this.viewManager = Preconditions.checkNotNull(viewManager, "viewManager");
    this.cacheExec = ScheduledExecutors.fixed(1, "DruidSchema-Cache-%d");
    this.tables = Maps.newConcurrentMap();
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
                final Set<String> dataSources = Sets.newHashSet();

                try {
                  synchronized (lock) {
                    final long nextRefresh = new DateTime(lastRefresh).plus(config.getMetadataRefreshPeriod())
                                                                      .getMillis();

                    while (!(
                        isServerViewInitialized
                        && !dataSourcesNeedingRefresh.isEmpty()
                        && (refreshImmediately || nextRefresh < System.currentTimeMillis())
                    )) {
                      lock.wait(Math.max(1, nextRefresh - System.currentTimeMillis()));
                    }

                    dataSources.addAll(dataSourcesNeedingRefresh);
                    dataSourcesNeedingRefresh.clear();
                    lastRefresh = System.currentTimeMillis();
                    refreshImmediately = false;
                  }

                  // Refresh dataSources.
                  for (final String dataSource : dataSources) {
                    log.debug("Refreshing metadata for dataSource[%s].", dataSource);
                    final long startTime = System.currentTimeMillis();
                    final DruidTable druidTable = computeTable(dataSource);
                    if (druidTable == null) {
                      if (tables.remove(dataSource) != null) {
                        log.info("Removed dataSource[%s] from the list of active dataSources.", dataSource);
                      }
                    } else {
                      tables.put(dataSource, druidTable);
                      log.info(
                          "Refreshed metadata for dataSource[%s] in %,dms.",
                          dataSource,
                          System.currentTimeMillis() - startTime
                      );
                    }
                  }

                  initializationLatch.countDown();
                }
                catch (InterruptedException e) {
                  // Fall through.
                  throw e;
                }
                catch (Exception e) {
                  log.warn(
                      e,
                      "Metadata refresh failed for dataSources[%s], trying again soon.",
                      Joiner.on(", ").join(dataSources)
                  );

                  synchronized (lock) {
                    // Add dataSources back to the refresh list.
                    dataSourcesNeedingRefresh.addAll(dataSources);
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

    serverView.registerSegmentCallback(
        MoreExecutors.sameThreadExecutor(),
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentViewInitialized()
          {
            synchronized (lock) {
              isServerViewInitialized = true;
              lock.notifyAll();
            }

            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            synchronized (lock) {
              dataSourcesNeedingRefresh.add(segment.getDataSource());
              if (!tables.containsKey(segment.getDataSource())) {
                refreshImmediately = true;
              }

              lock.notifyAll();
            }

            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
          {
            synchronized (lock) {
              dataSourcesNeedingRefresh.add(segment.getDataSource());
              lock.notifyAll();
            }

            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    serverView.registerServerCallback(
        MoreExecutors.sameThreadExecutor(),
        new ServerView.ServerCallback()
        {
          @Override
          public ServerView.CallbackAction serverRemoved(DruidServer server)
          {
            final List<String> dataSourceNames = Lists.newArrayList();
            for (DruidDataSource druidDataSource : server.getDataSources()) {
              dataSourceNames.add(druidDataSource.getName());
            }

            synchronized (lock) {
              dataSourcesNeedingRefresh.addAll(dataSourceNames);
              lock.notifyAll();
            }

            return ServerView.CallbackAction.CONTINUE;
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
  protected Multimap<String, Function> getFunctionMultimap()
  {
    final ImmutableMultimap.Builder<String, Function> builder = ImmutableMultimap.builder();
    for (Map.Entry<String, DruidViewMacro> entry : viewManager.getViews().entrySet()) {
      builder.put(entry);
    }
    return builder.build();
  }

  private DruidTable computeTable(final String dataSource)
  {
    final SegmentMetadataQuery segmentMetadataQuery = new SegmentMetadataQuery(
        new TableDataSource(dataSource),
        null,
        null,
        false,
        ImmutableMap.<String, Object>of("useCache", false, "populateCache", false),
        EnumSet.of(SegmentMetadataQuery.AnalysisType.INTERVAL),
        null,
        true
    );

    final Sequence<SegmentAnalysis> sequence = segmentMetadataQuery.run(walker, Maps.<String, Object>newHashMap());
    final List<SegmentAnalysis> results = Sequences.toList(sequence, Lists.<SegmentAnalysis>newArrayList());
    if (results.isEmpty()) {
      return null;
    }

    final Map<String, ValueType> columnTypes = Maps.newLinkedHashMap();

    // Resolve conflicts by taking the latest metadata. This aids in gradual schema evolution.
    long maxTimestamp = JodaUtils.MIN_INSTANT;

    for (SegmentAnalysis analysis : results) {
      final long timestamp;

      if (analysis.getIntervals() != null && analysis.getIntervals().size() > 0) {
        timestamp = analysis.getIntervals().get(analysis.getIntervals().size() - 1).getEndMillis();
      } else {
        timestamp = JodaUtils.MIN_INSTANT;
      }

      for (Map.Entry<String, ColumnAnalysis> entry : analysis.getColumns().entrySet()) {
        if (entry.getValue().isError()) {
          // Skip columns with analysis errors.
          continue;
        }

        if (!columnTypes.containsKey(entry.getKey()) || timestamp >= maxTimestamp) {
          ValueType valueType;
          try {
            valueType = ValueType.valueOf(entry.getValue().getType().toUpperCase());
          }
          catch (IllegalArgumentException e) {
            // Assume unrecognized types are some flavor of COMPLEX. This throws away information about exactly
            // what kind of complex column it is, which we may want to preserve some day.
            valueType = ValueType.COMPLEX;
          }

          columnTypes.put(entry.getKey(), valueType);

          maxTimestamp = timestamp;
        }
      }
    }

    final RowSignature.Builder rowSignature = RowSignature.builder();
    for (Map.Entry<String, ValueType> entry : columnTypes.entrySet()) {
      rowSignature.add(entry.getKey(), entry.getValue());
    }

    return new DruidTable(
        new TableDataSource(dataSource),
        rowSignature.build()
    );
  }
}
