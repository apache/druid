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

import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.client.MetadataSegmentView.PublishedSegmentCallback;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegmentChange;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <p>
 * Maintains a timeline of segments per datasource.
 * This timeline is populated by callback received from datanodes when they load a segment
 * or when realtime segments are created in a task.
 * Downstream classes can also register timeline callback on this class, for example BrokerSegmentMetadataCache.
 * </p>
 * <p>
 * There is a second flow which is enabled only when unavailabe segment detection is turned on.
 * {@link MetadataSegmentView} polls published segment metadata from the Coordinator. This class listens for published
 * segment updates.
 * The segment which are marked as `loaded` (used segment with non-zero replication factor which
 * has been once loaded onto some historical) is added to the timeline. This is basically the set of segments that
 * should be available for querying.
 * </p>
 * <p>
 * Lifecycle for a segment s,
 * <br>- s is created at time t1, ie. metadata for it is published in the database at time t1
 * <br>- coordinator polls it at time t2, at this point the segment s is not considered as loaded
 * <br>- broker polls coordinator at time t3 and finds segment s, but it is not added to the timeline since it is not loaded
 * <br>- historical loads s at time t4
 * <br>- coordinator and broker both receives callback from the historical
 * <br>- coordinator marks the segment as `loaded` at time t5
 * </p>
 * now, lets consider two possibilities,
 * <ol>
 * <li>Broker receives segment metadata for s from coordinator before the callback from historical
 * <ul>
 *   <li>
 *     broker adds s to the timeline at t6 with `queryable` field set to true,
 *     at this point any query that uses s would find that s is unavailable
 *   </li>
 *   <li>
 *     broker recieves callback for s from some historical at t7,
 *     now on queries using s would run fine
 *   </li>
 *   <li>
 *     if all historicals serving s goes down, the segment would still be present in the timeline marked as `queryable`
 *     and all queries using s would find s to be unavailable
 *   </li>
 * </ul>
 * </li>
 * <li>Broker first receives callback from historical and then segment metadata from the coordinator
 * <ul>
 *   <li>
 *     s is added to the timeline at t6 but it is not marked as `queryable`, however this doesn't affect queries using it
 *   </li>
 *   <li>Broker receives metadata from coordinator at t7, at this point it is marked as `queryable`</li>
 *   <li>
 *     if all historicals serving s goes down, the segment would still be present in the timeline marked as `queryable`
 *     and all queries using s would find s to be unavailable
 *   </li>
 * </ul>
 * </li>
 * </ol>
 * </p>
 */
@ManageLifecycle
public class BrokerServerView implements TimelineServerView
{
  private static final Logger log = new Logger(BrokerServerView.class);

  private final Object lock = new Object();
  private final ConcurrentMap<String, QueryableDruidServer> clients = new ConcurrentHashMap<>();
  private final Map<SegmentId, ServerSelector> selectors = new HashMap<>();
  private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines = new HashMap<>();
  private final ConcurrentMap<TimelineCallback, Executor> timelineCallbacks = new ConcurrentHashMap<>();
  private final QueryableDruidServer.Maker druidClientFactory;
  private final TierSelectorStrategy tierSelectorStrategy;
  private final ServiceEmitter emitter;
  private final BrokerSegmentWatcherConfig segmentWatcherConfig;
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> segmentFilter;
  private final CountDownLatch initialized = new CountDownLatch(1);
  private final FilteredServerInventoryView baseView;
  private final BrokerViewOfCoordinatorConfig brokerViewOfCoordinatorConfig;

  /**
   * if detectUnavailableSegments is true,
   * this latch ensures that we wait for published segment metadata to be polled from the Coordinator.
   */
  private final CountDownLatch publishedSegmentsMetadataInitialized;

  @Inject
  public BrokerServerView(
      final QueryableDruidServer.Maker directDruidClientFactory,
      final FilteredServerInventoryView baseView,
      final TierSelectorStrategy tierSelectorStrategy,
      final ServiceEmitter emitter,
      final BrokerSegmentWatcherConfig segmentWatcherConfig,
      final BrokerViewOfCoordinatorConfig brokerViewOfCoordinatorConfig,
      final MetadataSegmentView metadataSegmentView
  )
  {
    this.druidClientFactory = directDruidClientFactory;
    this.baseView = baseView;
    this.tierSelectorStrategy = tierSelectorStrategy;
    this.emitter = emitter;
    this.brokerViewOfCoordinatorConfig = brokerViewOfCoordinatorConfig;

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

          @Override
          public CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
          {
            return CallbackAction.CONTINUE;
          }
        },
        segmentFilter
    );

    baseView.registerServerCallback(
        exec,
        new ServerCallback() {
          @Override
          public CallbackAction serverAdded(DruidServer server)
          {
            // We don't track brokers in this view.
            if (!server.getType().equals(ServerType.BROKER)) {
              addServer(server);
            }
            return CallbackAction.CONTINUE;
          }

          @Override
          public CallbackAction serverRemoved(DruidServer server)
          {
            removeServer(server);
            return CallbackAction.CONTINUE;
          }
        }
    );

    if (segmentWatcherConfig.detectUnavailableSegments()) {
      metadataSegmentView.registerSegmentCallback(
          exec,
          new PublishedSegmentCallback()
          {
            @Override
            public void fullSync(List<DataSegmentChange> segments)
            {
              publishedSegmentsFullSync(segments);
            }

            @Override
            public void deltaSync(List<DataSegmentChange> segments)
            {
              publishedSegmentsDeltaSync(segments);
            }

            @Override
            public void segmentViewInitialized()
            {
              publishedSegmentsMetadataInitialized.countDown();
            }
          }
      );
      publishedSegmentsMetadataInitialized = new CountDownLatch(1);
    } else {
      publishedSegmentsMetadataInitialized = new CountDownLatch(0);
    }
  }

  @LifecycleStart
  public void start() throws InterruptedException
  {
    if (segmentWatcherConfig.isAwaitInitializationOnStart()) {
      final long startMillis = System.currentTimeMillis();
      log.info("BrokerServerView waiting for initialization.");
      awaitInitialization();
      final long endMillis = System.currentTimeMillis();
      log.info("BrokerServerView initialized in [%,d] ms.", endMillis - startMillis);
      emitter.emit(ServiceMetricEvent.builder().setMetric(
          "serverview/init/time",
          endMillis - startMillis
      ));
    }
  }

  public boolean isInitialized()
  {
    return initialized.getCount() == 0 && publishedSegmentsMetadataInitialized.getCount() == 0;
  }

  public void awaitInitialization() throws InterruptedException
  {
    initialized.await();
    publishedSegmentsMetadataInitialized.await();
  }

  public QueryableDruidServer.Maker getDruidClientFactory()
  {
    return druidClientFactory;
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
    QueryableDruidServer retVal = druidClientFactory.make(server);
    QueryableDruidServer exists = clients.put(server.getName(), retVal);
    if (exists != null) {
      log.warn("QueryRunner for server[%s] already exists!? Well it's getting replaced", server);
    }

    return retVal;
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
    final SegmentId segmentId = segment.getId();
    synchronized (lock) {
      boolean runCallBack = !segmentWatcherConfig.detectUnavailableSegments();

      // in theory we could probably just filter this to ensure we don't put ourselves in here, to make broker tree
      // query topologies, but for now just skip all brokers, so we don't create some sort of wild infinite query
      // loop...
      if (!server.getType().equals(ServerType.BROKER)) {
        log.debug("Adding segment[%s] for server[%s]", segment, server);

        ServerSelector selector = selectors.get(segmentId);
        if (selector == null) {
          // if unavailableSegmentDetection is enabled, segment is not queryable unless added by coordinator
          selector = new ServerSelector(segment, tierSelectorStrategy, brokerViewOfCoordinatorConfig, false);

          VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
          if (timeline == null) {
            // broker needs to skip tombstones
            timeline = new VersionedIntervalTimeline<>(Ordering.natural(), true);
            timelines.put(segment.getDataSource(), timeline);
          }

          timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
          selectors.put(segmentId, selector);
        } else {
          // implies that segment was already present, if it is queryable run callback
          runCallBack = runCallBack || selector.isQueryable();
        }

        QueryableDruidServer queryableDruidServer = clients.get(server.getName());
        if (queryableDruidServer == null) {
          DruidServer inventoryValue = baseView.getInventoryValue(server.getName());
          if (inventoryValue == null) {
            log.warn(
                "Could not find server[%s] in inventory. Skipping addition of segment[%s].",
                server.getName(),
                segmentId
            );
            return;
          } else {
            queryableDruidServer = addServer(inventoryValue);
          }
        }
        selector.addServerAndUpdateSegment(queryableDruidServer, segment);
      }

      if (runCallBack) {
        // run the callbacks, even if the segment came from a broker, lets downstream watchers decide what to do with it
        runTimelineCallbacks(callback -> callback.segmentAdded(server, segment));
      }
    }
  }

  private void serverRemovedSegment(DruidServerMetadata server, DataSegment segment)
  {
    final SegmentId segmentId = segment.getId();
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
      if (queryableDruidServer == null) {
        log.warn(
            "Could not find server[%s] in inventory. Skipping removal of segment[%s].",
            server.getName(),
            segmentId
        );
      } else if (!selector.removeServer(queryableDruidServer)) {
        log.warn(
            "Asked to disassociate non-existant association between server[%s] and segment[%s]",
            server,
            segmentId
        );
      } else {
        runTimelineCallbacks(callback -> callback.serverSegmentRemoved(server, segment));
      }

      if (selector.isEmpty()) {
        selectors.remove(segmentId);

        // if unavailableSegmentDetection is enabled, the segment is removed on coordinator sync
        if (!segmentWatcherConfig.detectUnavailableSegments()) {
          VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
          final PartitionChunk<ServerSelector> removedPartition = timeline.remove(
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
  }

  private void publishedSegmentsFullSync(final List<DataSegmentChange> dataSegmentChanges)
  {
    Map<String, Map<SegmentId, SegmentStatusInCluster>> loadedSegmentsPerDataSource = new HashMap<>();

    int addedSegmentCount = 0, loadedSegmentCount = 0;
    for (DataSegmentChange dataSegmentChange : dataSegmentChanges) {
      addedSegmentCount++;
      SegmentStatusInCluster segmentStatusInCluster =
          dataSegmentChange.getSegmentStatusInCluster();
      if (segmentStatusInCluster.isLoaded()) {
        loadedSegmentCount++;
        loadedSegmentsPerDataSource.computeIfAbsent(
            segmentStatusInCluster.getDataSegment().getDataSource(),
            value -> new HashMap<>()
        ).put(segmentStatusInCluster.getDataSegment().getId(), segmentStatusInCluster);
      }
    }

    emitter.emit(ServiceMetricEvent.builder().setMetric(
        "query/changedSegments/add",
        addedSegmentCount
    ));

    emitter.emit(ServiceMetricEvent.builder().setMetric(
        "query/changedSegments/loaded",
        loadedSegmentCount
    ));

    synchronized (lock) {
       // For each datasource lets assume,
       // `loadedSegments` is the set of segments in the system which are loaded
       // `timelineSegments` is the set of segments in the current timeline
       // To sync the timeline with the state of segments in the cluster,
       // Get rid of unused segments, remove `timelineSegments` - `loadedSegments` from the timeline
       // Add loaded but unavailable segments, add `loadedSegments` - `timelineSegments` to the timeline
      for (Map.Entry<String, Map<SegmentId, SegmentStatusInCluster>> entry : loadedSegmentsPerDataSource.entrySet()) {
        String dataSource = entry.getKey();
        Map<SegmentId, SegmentStatusInCluster> loadedSegments = entry.getValue();

        VersionedIntervalTimeline<String, ServerSelector> versionedIntervalTimeline = timelines.get(dataSource);

        Map<SegmentId, VersionedIntervalTimeline.PartitionChunkEntry<String, ServerSelector>>
            segmentIdPartitionChunkEntryMap =
            versionedIntervalTimeline
                .iterateAllEntries()
                .stream()
                .collect(Collectors.toMap(
                    pce -> pce.getChunk().getObject().getSegment().getId(),
                    Function.identity()
                ));

        // these segments were not present in the set of polled segments from the Coordinator
        Set<SegmentId> segmentIdsToRemoveFromTimeline =
            Sets.difference(segmentIdPartitionChunkEntryMap.keySet(), loadedSegments.keySet());

        // remove segments from the timeline
        segmentIdsToRemoveFromTimeline.forEach(
            segmentId -> removeSegmentFromTimeline(
                segmentIdPartitionChunkEntryMap
                    .get(segmentId)
                    .getChunk()
                    .getObject()
                    .getSegment()));

        // add all the loaded segment to the timeline, this would add segments which are loaded but unavailable
        addSegmentsToTimeline(new ArrayList<>(entry.getValue().values()));
      }
    }
  }

  private void publishedSegmentsDeltaSync(final List<DataSegmentChange> dataSegmentChanges)
  {
    List<SegmentStatusInCluster> segmentsToAdd = new ArrayList<>();
    List<DataSegment> segmentsToRemove = new ArrayList<>();
    int addedSegmentCount = 0, removedSegmentCount = 0, loadedSegmentCount = 0;

    for (DataSegmentChange dataSegmentChange : dataSegmentChanges) {
      if (!dataSegmentChange.isRemoved()) {
        addedSegmentCount++;
        if (dataSegmentChange.getSegmentStatusInCluster().isLoaded()) {
          loadedSegmentCount++;
          segmentsToAdd.add(dataSegmentChange.getSegmentStatusInCluster());
        }
      } else {
        removedSegmentCount++;
        if (dataSegmentChange.getChangeType() == DataSegmentChange.SegmentLifecycleChangeType.SEGMENT_REMOVED) {
          segmentsToRemove.add(dataSegmentChange.getSegmentStatusInCluster().getDataSegment());
        }
      }
    }

    emitter.emit(ServiceMetricEvent.builder().setMetric(
        "query/changedSegments/add",
        addedSegmentCount
    ));

    emitter.emit(ServiceMetricEvent.builder().setMetric(
        "query/changedSegments/loaded",
        loadedSegmentCount
    ));

    emitter.emit(ServiceMetricEvent.builder().setMetric(
        "query/changedSegments/remove",
        removedSegmentCount
    ));

    synchronized (lock) {
      // remove the segment from the timeline
      segmentsToRemove.forEach(this::removeSegmentFromTimeline);

      // add the segment to the timeline
      addSegmentsToTimeline(segmentsToAdd);
    }
  }

  private void addSegmentsToTimeline(List<SegmentStatusInCluster> segments)
  {
    Map<String, List<VersionedIntervalTimeline.PartitionChunkEntry<String, ServerSelector>>>
        partitionChunkEntryMap = new HashMap<>();

    for (SegmentStatusInCluster segmentPlus : segments) {
      DataSegment segment = segmentPlus.getDataSegment();
      SegmentId segmentId = segment.getId();
      ServerSelector selector = selectors.get(segmentId);
      if (selector == null) {
        log.info("selector is null for segment [%s]", segmentId);
        selector = new ServerSelector(segment, tierSelectorStrategy, brokerViewOfCoordinatorConfig, true);

        VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
        if (timeline == null) {
          // broker needs to skip tombstones
          timeline = new VersionedIntervalTimeline<>(Ordering.natural(), true);
          timelines.put(segment.getDataSource(), timeline);
        }

        partitionChunkEntryMap
            .computeIfAbsent(segment.getDataSource(), entries -> new ArrayList<>())
            .add(new VersionedIntervalTimeline.PartitionChunkEntry<>(
                segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector)));
        selectors.put(segmentId, selector);
      } else {
        // if segment was already added by historical, mark it queryable
        selector.setQueryable(true);
        // run call back for all server this segment is present on
        for (DruidServerMetadata druidServer : selector.getAllServers(CloneQueryMode.INCLUDECLONES)) {
          runTimelineCallbacks(callback -> callback.segmentAdded(druidServer, segment));
        }
      }
    }

    partitionChunkEntryMap.forEach((dataSource, entries) -> timelines.get(dataSource).addAll(entries.iterator()));
  }

  private void removeSegmentFromTimeline(DataSegment segment)
  {
    SegmentId segmentId = segment.getId();

    ServerSelector selector = selectors.get(segmentId);
    if (selector == null) {
      log.warn("Told to remove non-existant segment[%s]", segmentId);
      return;
    }

    // run call back for all server this segment is present on
    for (DruidServerMetadata server : selector.getAllServers(CloneQueryMode.INCLUDECLONES)) {
      runTimelineCallbacks(callback -> callback.serverSegmentRemoved(server, segment));
    }

    selectors.remove(segmentId);

    VersionedIntervalTimeline<String, ServerSelector> versionedIntervalTimeline =
        timelines.get(segment.getDataSource());

    final PartitionChunk<ServerSelector> removedPartition =
        versionedIntervalTimeline.remove(
            segment.getInterval(),
            segment.getVersion(),
            segment.getShardSpec().createChunk(selector)
        );

    if (removedPartition == null) {
      log.warn(
          "Asked to remove timeline entry[interval: %s, version: %s] that doesn't exist",
          segment.getInterval(),
          segment.getVersion());
    } else {
      runTimelineCallbacks(callback -> callback.segmentRemoved(segment));
    }
  }

  @Override
  public Optional<VersionedIntervalTimeline<String, ServerSelector>> getTimeline(final TableDataSource dataSource)
  {
    synchronized (lock) {
      return Optional.ofNullable(timelines.get(dataSource.getName()));
    }
  }

  @Override
  public void registerTimelineCallback(final Executor exec, final TimelineCallback callback)
  {
    timelineCallbacks.put(callback, exec);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> QueryRunner<T> getQueryRunner(DruidServer server)
  {
    synchronized (lock) {
      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (queryableDruidServer == null) {
        log.error("No QueryRunner found for server name[%s].", server.getName());
        return null;
      }
      return (QueryRunner<T>) queryableDruidServer.getQueryRunner();
    }
  }

  @Override
  public void registerServerCallback(Executor exec, ServerCallback callback)
  {
    baseView.registerServerCallback(exec, callback);
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
  public List<DruidServerMetadata> getDruidServerMetadatas()
  {
    // Override default implementation for better performance.
    final List<DruidServerMetadata> retVal = new ArrayList<>(clients.size());

    for (final QueryableDruidServer server : clients.values()) {
      retVal.add(server.getServer().getMetadata());
    }

    return retVal;
  }

  @Override
  public List<ImmutableDruidServer> getDruidServers()
  {
    return clients.values().stream()
                  .map(queryableDruidServer -> queryableDruidServer.getServer().toImmutableDruidServer())
                  .collect(Collectors.toList());
  }
}
