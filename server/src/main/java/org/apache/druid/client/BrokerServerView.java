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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.*;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.ComplementaryNamespacedVersionedIntervalTimeline;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.NamespacedVersionedIntervalTimeline;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 */
@ManageLifecycle
public class BrokerServerView implements TimelineServerView
{
  private static final Logger log = new Logger(BrokerServerView.class);

  private final Object lock = new Object();

  private final ConcurrentMap<String, QueryableDruidServer> clients;
  private final Map<SegmentId, ServerSelector> selectors;
  private final Map<String, NamespacedVersionedIntervalTimeline<String, ServerSelector>> timelines;
  private final ConcurrentMap<TimelineCallback, Executor> timelineCallbacks = new ConcurrentHashMap<>();

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final FilteredServerInventoryView baseView;
  private final TierSelectorStrategy tierSelectorStrategy;
  private final ServiceEmitter emitter;
  private final BrokerSegmentWatcherConfig segmentWatcherConfig;
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> segmentFilter;

  private final Map<String, List<String>> dataSourceComplementaryMapToQueryOrder;
  private final Map<String, List<String>> dataSourceComplementaryReverseMapToQueryOrder;

  private final CountDownLatch initialized = new CountDownLatch(1);

  @Inject
  public BrokerServerView(
          final QueryToolChestWarehouse warehouse,
          final QueryWatcher queryWatcher,
          final @Smile ObjectMapper smileMapper,
          final @EscalatedClient HttpClient httpClient,
          final FilteredServerInventoryView baseView,
          final TierSelectorStrategy tierSelectorStrategy,
          final ServiceEmitter emitter,
          final BrokerSegmentWatcherConfig segmentWatcherConfig,
          final BrokerDataSourceComplementConfig dataSourceComplementConfig,
          final BrokerDataSourceMultiComplementConfig dataSourceMultiComplementConfig)
  {
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.baseView = baseView;
    this.tierSelectorStrategy = tierSelectorStrategy;
    this.emitter = emitter;
   // TODO (lucilla) should be removed after we fully migrate to using BrokerDataSourceMultiComplementConfig
    this.dataSourceComplementaryMapToQueryOrder = CollectionUtils.mapValues(dataSourceComplementConfig.getMapping(), Collections::singletonList);
    Map<String, List<String>> dataSourceMultiComplementConfigMapping = dataSourceMultiComplementConfig.getMapping();
    dataSourceMultiComplementConfigMapping.keySet().forEach(key ->
            dataSourceComplementaryMapToQueryOrder.putIfAbsent(key, dataSourceMultiComplementConfigMapping.get(key))
    );

    this.dataSourceComplementaryReverseMapToQueryOrder = new HashMap<>();
    dataSourceComplementaryMapToQueryOrder.forEach((dataSource, supportDataSources) -> {
      supportDataSources.forEach(supportDataSource -> {
        List<String> dependentDataSources = dataSourceComplementaryReverseMapToQueryOrder.getOrDefault(
                supportDataSource,
                new ArrayList<>());
        dependentDataSources.add(dataSource);
        dataSourceComplementaryReverseMapToQueryOrder.put(supportDataSource, dependentDataSources);
      });
    });

    this.clients = new ConcurrentHashMap<>();
    this.selectors = new HashMap<>();
    this.timelines = new HashMap<>();

    // Validate and set the segment watcher config
    validateSegmentWatcherConfig(segmentWatcherConfig);
    this.segmentWatcherConfig = segmentWatcherConfig;

    this.segmentFilter = (Pair<DruidServerMetadata, DataSegment> metadataAndSegment) -> {

      // Include only watched tiers if specified
      if (segmentWatcherConfig.getWatchedTiers() != null
          && !segmentWatcherConfig.getWatchedTiers().contains(metadataAndSegment.lhs.getTier())) {
        return false;
      }

      // Exclude ignored tiers if specified
      if (segmentWatcherConfig.getIgnoredTiers() != null
          && segmentWatcherConfig.getIgnoredTiers().contains(metadataAndSegment.lhs.getTier())) {
        return false;
      }

      // Include only watched datasources if specified
      if (segmentWatcherConfig.getWatchedDataSources() != null
          && !segmentWatcherConfig.getWatchedDataSources().contains(metadataAndSegment.rhs.getDataSource())) {
        return false;
      }

      // Include realtime tasks only if they are watched
      return metadataAndSegment.lhs.getType() != ServerType.INDEXER_EXECUTOR
             || segmentWatcherConfig.isWatchRealtimeTasks();
    };
    ExecutorService exec = Execs.singleThreaded("BrokerServerView-%s");
    baseView.registerSegmentCallback(
        exec,
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            serverAddedSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(final DruidServerMetadata server, DataSegment segment)
          {
            serverRemovedSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public CallbackAction segmentViewInitialized()
          {
            initialized.countDown();
            runTimelineCallbacks(TimelineCallback::timelineInitialized);
            return ServerView.CallbackAction.CONTINUE;
          }
        },
        segmentFilter
    );

    baseView.registerServerRemovedCallback(
        exec,
        server -> {
          removeServer(server);
          return CallbackAction.CONTINUE;
        }
    );
  }

  @LifecycleStart
  public void start() throws InterruptedException
  {
    if (segmentWatcherConfig.isAwaitInitializationOnStart()) {
      final long startNanos = System.nanoTime();
      log.debug("%s waiting for initialization.", getClass().getSimpleName());
      awaitInitialization();
      log.info("%s initialized in [%,d] ms.", getClass().getSimpleName(), (System.nanoTime() - startNanos) / 1000000);
    }
  }

  public boolean isInitialized()
  {
    return initialized.getCount() == 0;
  }

  public void awaitInitialization() throws InterruptedException
  {
    initialized.await();
  }

  /**
   * Validates the given BrokerSegmentWatcherConfig.
   * <ul>
   *   <li>At most one of watchedTiers or ignoredTiers can be set</li>
   *   <li>If set, watchedTiers must be non-empty</li>
   *   <li>If set, ignoredTiers must be non-empty</li>
   * </ul>
   */
  private void validateSegmentWatcherConfig(BrokerSegmentWatcherConfig watcherConfig)
  {
    if (watcherConfig.getWatchedTiers() != null
        && watcherConfig.getIgnoredTiers() != null) {
      throw new ISE(
          "At most one of 'druid.broker.segment.watchedTiers' "
          + "and 'druid.broker.segment.ignoredTiers' can be configured."
      );
    }

    if (watcherConfig.getWatchedTiers() != null
        && watcherConfig.getWatchedTiers().isEmpty()) {
      throw new ISE("If configured, 'druid.broker.segment.watchedTiers' must be non-empty");
    }

    if (watcherConfig.getIgnoredTiers() != null
        && watcherConfig.getIgnoredTiers().isEmpty()) {
      throw new ISE("If configured, 'druid.broker.segment.ignoredTiers' must be non-empty");
    }
  }

  private QueryableDruidServer addServer(DruidServer server)
  {
    QueryableDruidServer retVal = new QueryableDruidServer<>(server, makeDirectClient(server));
    QueryableDruidServer exists = clients.put(server.getName(), retVal);
    if (exists != null) {
      log.warn("QueryRunner for server[%s] already exists!? Well it's getting replaced", server);
    }

    return retVal;
  }

  private DirectDruidClient makeDirectClient(DruidServer server)
  {
    return new DirectDruidClient(
        warehouse,
        queryWatcher,
        smileMapper,
        httpClient,
        server.getScheme(),
        server.getHost(),
        emitter
    );
  }

  private QueryableDruidServer removeServer(DruidServer server)
  {
    for (DataSegment segment : server.iterateAllSegments()) {
      serverRemovedSegment(server.getMetadata(), segment);
    }
    return clients.remove(server.getName());
  }

  private void serverAddedSegment(final DruidServerMetadata server, final DataSegment segment)
  {
    SegmentId segmentId = segment.getId();
    synchronized (lock) {
      log.debug("Adding segment[%s] for server[%s]", segment, server);

      ServerSelector selector = selectors.get(segmentId);
      if (selector == null) {
        selector = new ServerSelector(segment, tierSelectorStrategy);

        String dataSource = segment.getDataSource();
        NamespacedVersionedIntervalTimeline<String, ServerSelector> timeline = timelines
                .get(dataSource);
        if (timeline == null) {
          if (dataSourceComplementaryMapToQueryOrder.containsKey(dataSource)) {
            Map<String, NamespacedVersionedIntervalTimeline<String, ServerSelector>> supportTimelinesByDataSource = new HashMap<>();
            for (String supportDataSource : dataSourceComplementaryMapToQueryOrder.get(dataSource)) {
              NamespacedVersionedIntervalTimeline<String, ServerSelector> supportTimeline = timelines.get(supportDataSource);
              if (supportTimeline == null) {
                supportTimeline = new NamespacedVersionedIntervalTimeline<>(Ordering.natural());
                timelines.put(supportDataSource, supportTimeline);
              }
              supportTimelinesByDataSource.put(supportDataSource, supportTimeline);
            }
            timeline = new ComplementaryNamespacedVersionedIntervalTimeline(
                    dataSource,
                    supportTimelinesByDataSource,
                    dataSourceComplementaryMapToQueryOrder.get(dataSource));

          } else {
            timeline = new NamespacedVersionedIntervalTimeline<>(Ordering.natural());
            if (dataSourceComplementaryReverseMapToQueryOrder.containsKey(dataSource)) {
              for (String dependentDataSource : dataSourceComplementaryReverseMapToQueryOrder.get(dataSource)) {
                NamespacedVersionedIntervalTimeline<String, ServerSelector> complementaryTimeline = timelines.get(dependentDataSource);
                if (complementaryTimeline == null) {
                  Map<String, NamespacedVersionedIntervalTimeline<String, ServerSelector>> supportTimelinesByDataSource = new HashMap<>();
                  supportTimelinesByDataSource.put(dataSource, timeline);
                  complementaryTimeline = new ComplementaryNamespacedVersionedIntervalTimeline(
                          dependentDataSource,
                          supportTimelinesByDataSource,
                          dataSourceComplementaryMapToQueryOrder.get(dependentDataSource));
                }
                timelines.put(dependentDataSource, complementaryTimeline);
              }
            }
            timeline = new ComplementaryNamespacedVersionedIntervalTimeline(
                    dataSource,
                    supportTimelinesByDataSource,
                    dataSourceComplementaryMapToQueryOrder.get(dataSource));

          } else {
            timeline = new NamespacedVersionedIntervalTimeline<>(Ordering.natural());
            if (dataSourceComplementaryReverseMapToQueryOrder.containsKey(dataSource)) {
              for (String dependentDataSource : dataSourceComplementaryReverseMapToQueryOrder.get(dataSource)) {
                NamespacedVersionedIntervalTimeline<String, ServerSelector> complementaryTimeline = timelines.get(dependentDataSource);
                if (complementaryTimeline == null) {
                  Map<String, NamespacedVersionedIntervalTimeline<String, ServerSelector>> supportTimelinesByDataSource = new HashMap<>();
                  supportTimelinesByDataSource.put(dataSource, timeline);
                  complementaryTimeline = new ComplementaryNamespacedVersionedIntervalTimeline(
                          dependentDataSource,
                          supportTimelinesByDataSource,
                          dataSourceComplementaryMapToQueryOrder.get(dependentDataSource));
                }
                timelines.put(dependentDataSource, complementaryTimeline);
              }
            }
          }

          timeline.add(
                  NamespacedVersionedIntervalTimeline.getNamespace(
                          segment.getShardSpec().getIdentifier()),
                  segment.getInterval(),
                  segment.getVersion(),
                  segment.getShardSpec().createChunk(selector));
          selectors.put(segmentId, selector);
        }

        timeline.add(
                NamespacedVersionedIntervalTimeline.getNamespace(
                        segment.getShardSpec().getIdentifier()),
                segment.getInterval(),
                segment.getVersion(),
                segment.getShardSpec().createChunk(selector));
        selectors.put(segmentId, selector);
      }
      // run the callbacks, even if the segment came from a broker, lets downstream watchers decide what to do with it
      runTimelineCallbacks(callback -> callback.segmentAdded(server, segment));
    }
  }

  private void serverRemovedSegment(DruidServerMetadata server, DataSegment segment)
  {

    SegmentId segmentId = segment.getId();
    final ServerSelector selector;

    synchronized (lock) {
      log.debug("Removing segment[%s] from server[%s].", segmentId, server);

      // we don't store broker segments here, but still run the callbacks for the segment being removed from the server
      // since the broker segments are not stored on the timeline, do not fire segmentRemoved event
      if (server.getType().equals(ServerType.BROKER)) {
        runTimelineCallbacks(callback -> callback.serverSegmentRemoved(server, segment));
        return;
      }

      selector = selectors.get(segmentId);
      if (selector == null) {
        log.warn("Told to remove non-existant segment[%s]", segmentId);
        return;
      }

      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (!selector.removeServer(queryableDruidServer)) {
        log.warn(
            "Asked to disassociate non-existant association between server[%s] and segment[%s]",
            server,
            segmentId
        );
      } else {
        runTimelineCallbacks(callback -> callback.serverSegmentRemoved(server, segment));
      }

      if (selector.isEmpty()) {
        NamespacedVersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
        selectors.remove(segmentId);

        final PartitionChunk<ServerSelector> removedPartition = timeline.remove(
            NamespacedVersionedIntervalTimeline.getNamespace(segment.getShardSpec().getIdentifier()),
            segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector)
        );

        if (removedPartition == null) {
          log.warn(
              "Asked to remove timeline entry[interval: %s, version: %s] that doesn't exist",
              segment.getInterval(),
              segment.getVersion()
          );
        } else {
          runTimelineCallbacks(callback -> callback.segmentRemoved(segment));
        }
      }
    }
  }

  @Override
  public Optional<NamespacedVersionedIntervalTimeline<String, ServerSelector>> getTimeline(final DataSourceAnalysis analysis)
  {
    final TableDataSource table =
        analysis.getBaseTableDataSource()
                .orElseThrow(() -> new ISE("Cannot handle datasource: %s", analysis.getDataSource()));

    synchronized (lock) {
      return Optional.ofNullable(timelines.get(table.getName()));
    }
  }

  public NamespacedVersionedIntervalTimeline<String, ServerSelector> getTimeline(DataSource dataSource)
  {
    String table = Iterables.getOnlyElement(dataSource.getTableNames());
    synchronized (lock) {
      return timelines.get(table);
    }
  }

  @Override
  public void registerTimelineCallback(final Executor exec, final TimelineCallback callback)
  {
    timelineCallbacks.put(callback, exec);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(DruidServer server)
  {
    synchronized (lock) {
      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (queryableDruidServer == null) {
        log.error("No QueryableDruidServer found for %s", server.getName());
        return null;
      }
      return queryableDruidServer.getQueryRunner();
    }
  }

  @Override
  public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
  {
    baseView.registerServerRemovedCallback(exec, callback);
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    baseView.registerSegmentCallback(exec, callback, segmentFilter);
  }

  private void runTimelineCallbacks(final Function<TimelineCallback, CallbackAction> function)
  {
    for (Map.Entry<TimelineCallback, Executor> entry : timelineCallbacks.entrySet()) {
      entry.getValue().execute(
          () -> {
            if (CallbackAction.UNREGISTER == function.apply(entry.getKey())) {
              timelineCallbacks.remove(entry.getKey());
            }
          }
      );
    }
  }

  @Override
  public List<ImmutableDruidServer> getDruidServers()
  {
    return clients.values().stream()
                  .map(queryableDruidServer -> queryableDruidServer.getServer().toImmutableDruidServer())
                  .collect(Collectors.toList());
  }
}
