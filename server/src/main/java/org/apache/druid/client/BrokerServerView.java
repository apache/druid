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
import com.google.inject.Inject;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 */
@ManageLifecycle
public class BrokerServerView implements TimelineServerView
{
  public static final String REALTIME_SELECTOR = "realtime";
  private static final Logger log = new Logger(BrokerServerView.class);

  private final Object lock = new Object();
  private final ConcurrentMap<String, QueryableDruidServer> clients = new ConcurrentHashMap<>();
  private final Map<SegmentId, ServerSelector> selectors = new HashMap<>();
  private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines = new HashMap<>();
  private final ConcurrentMap<TimelineCallback, Executor> timelineCallbacks = new ConcurrentHashMap<>();
  private final QueryableDruidServer.Maker druidClientFactory;
  private final TierSelectorStrategy historicalTierSelectorStrategy;
  private final TierSelectorStrategy realtimeTierSelectorStrategy;
  private final ServiceEmitter emitter;
  private final BrokerSegmentWatcherConfig segmentWatcherConfig;
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> segmentFilter;
  private final CountDownLatch initialized = new CountDownLatch(1);
  private final FilteredServerInventoryView baseView;
  private final BrokerViewOfCoordinatorConfig brokerViewOfCoordinatorConfig;

  /**
   * Executor for scheduling delayed segment removals from the timeline to prevent
   * the segment load/drop race condition (see {@link #pendingSegmentRemovals}).
   */
  private final ScheduledExecutorService delayedRemovalExecutor;

  /**
   * Map of segment IDs to their pending delayed cleanup futures. When the last server for a
   * segment is removed and a delay is configured, the segment is removed from the timeline
   * immediately (to prevent queries from seeing an empty selector) while the selector is kept
   * in the selectors map. A delayed cleanup of the selector is scheduled. If a new server
   * announces the same segment before the delay expires, the scheduled cleanup is cancelled
   * and the segment is re-added to the timeline. This prevents the segment load/drop race
   * described in <a href="https://github.com/apache/druid/issues/18738">#18738</a>.
   * <p>
   * The future stored here is an {@link EntryFuture} whose delegate is published by
   * {@link EntryFuture#setDelayedRemovalFuture(ScheduledFuture)} <em>after</em> the entry is
   * registered in this map, never before. That ordering lets the scheduled task (which cannot
   * be constructed before it is scheduled) reference its own entry directly and distinguish
   * "I am still the current entry" from "a newer entry replaced me" without relying on
   * self-referential lambda capture, which Java's definite-assignment rules reject.
   */
  private final ConcurrentHashMap<SegmentId, EntryFuture> pendingSegmentRemovals = new ConcurrentHashMap<>();

  @Inject
  public BrokerServerView(
      final QueryableDruidServer.Maker directDruidClientFactory,
      final FilteredServerInventoryView baseView,
      final TierSelectorStrategy historicalTierSelectorStrategy, // Injected from bindings configured in CliBroker
      @Named(REALTIME_SELECTOR) final TierSelectorStrategy realtimeTierSelectorStrategy, // Injected from bindings set up in BrokerRealtimeSelectorModule
      final ServiceEmitter emitter,
      final BrokerSegmentWatcherConfig segmentWatcherConfig,
      final BrokerViewOfCoordinatorConfig brokerViewOfCoordinatorConfig
  )
  {
    this.druidClientFactory = directDruidClientFactory;
    this.baseView = baseView;
    this.historicalTierSelectorStrategy = historicalTierSelectorStrategy;
    this.realtimeTierSelectorStrategy = realtimeTierSelectorStrategy;
    log.info("Using historicalTierSelectorStrategy[%s] and realtimeTierSelectorStrategy[%s]", historicalTierSelectorStrategy, realtimeTierSelectorStrategy);

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

    this.delayedRemovalExecutor = Execs.scheduledSingleThreaded("BrokerServerView-DelayedRemoval-%s");
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
        new ServerCallback()
        {
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

  @LifecycleStop
  public void stop()
  {
    log.info("BrokerServerView stopping. Cancelling [%d] pending segment removals.", pendingSegmentRemovals.size());
    for (EntryFuture future : pendingSegmentRemovals.values()) {
      future.cancel(false);
    }
    pendingSegmentRemovals.clear();
    delayedRemovalExecutor.shutdownNow();
    log.info("BrokerServerView stopped.");
  }

  public boolean isInitialized()
  {
    return initialized.getCount() == 0;
  }

  public void awaitInitialization() throws InterruptedException
  {
    initialized.await();
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
      // in theory we could probably just filter this to ensure we don't put ourselves in here, to make broker tree
      // query topologies, but for now just skip all brokers, so we don't create some sort of wild infinite query
      // loop...
      if (!server.getType().equals(ServerType.BROKER)) {
        log.debug("Adding segment[%s] for server[%s]", segment, server);

        // Cancel any pending delayed removal for this segment. If the last server was removed
        // and a delayed removal was scheduled, a new server announcing the segment means the
        // segment should stay in the timeline.
        final EntryFuture pendingRemoval = pendingSegmentRemovals.remove(segmentId);
        if (pendingRemoval != null) {
          pendingRemoval.cancel(false);
          log.debug("Cancelled pending removal for segment[%s] because a new server[%s] is announcing it.", segmentId, server.getName());
        }

        ServerSelector selector = selectors.get(segmentId);
        if (selector == null) {
          selector = new ServerSelector(segment, historicalTierSelectorStrategy, realtimeTierSelectorStrategy, brokerViewOfCoordinatorConfig);

          VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
          if (timeline == null) {
            // broker needs to skip tombstones
            timeline = new VersionedIntervalTimeline<>(Ordering.natural(), true);
            timelines.put(segment.getDataSource(), timeline);
          }

          timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
          selectors.put(segmentId, selector);
        } else {
          // The selector exists but may have been removed from the timeline during a
          // delayed drop. Re-add it to the timeline if needed.
          VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
          if (timeline == null) {
            timeline = new VersionedIntervalTimeline<>(Ordering.natural(), true);
            timelines.put(segment.getDataSource(), timeline);
          }
          // Only re-add if the selector is not already in the timeline (e.g., after a delayed drop).
          // The timeline.add is idempotent so this is safe to call unconditionally, but we check
          // isEmpty() as a heuristic to avoid unnecessary work.
          if (selector.isEmpty()) {
            timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
          }
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
      // run the callbacks, even if the segment came from a broker, lets downstream watchers decide what to do with it
      runTimelineCallbacks(callback -> callback.segmentAdded(server, segment));
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
        final long delayMillis = segmentWatcherConfig.getSegmentDropDelayMillis();
        if (delayMillis > 0) {
          // Remove the segment from the timeline immediately so that queries never observe an
          // empty ServerSelector on the timeline (an empty selector makes groupSegmentsByServer
          // skip the holder while computeUncoveredIntervals still counts it as covered, which
          // yields a silent partial result). The selector is kept in the selectors map so that
          // a new server announcing the segment during the delay can re-add it to the timeline.
          // segmentRemoved is not fired until the selector is actually cleaned up, because the
          // segment may come back before the delay expires.
          // See https://github.com/apache/druid/issues/18738
          removeFromTimelineQuietly(segment, selector);

          // Schedule the delayed cleanup of the selector from the selectors map. If a new server
          // announces the segment before the delay expires, the entry is cancelled and the
          // segment is re-added to the timeline.
          //
          // The callback closes over `entry`, which is created before scheduling and therefore
          // definitely assigned. Passing a *different* object to the scheduled task would half-fix
          // this: the entry identity is what lets the callback tell "I am still the current entry"
          // from "a newer entry replaced me", which a self-comparison of the future cannot do.
          final long scheduledAtNanos = System.nanoTime();
          final EntryFuture entry = new EntryFuture();
          entry.setDelayedRemovalFuture(
              delayedRemovalExecutor.schedule(
                  () -> runDelayedRemoval(segment, selector, entry, scheduledAtNanos),
                  delayMillis,
                  TimeUnit.MILLISECONDS
              )
          );

          // Register the entry as the current pending removal. The timer may already have fired
          // (delayMillis can be shorter than this thread's scheduling latency); that is safe
          // because the callback re-validates the entry under `lock` and bails out if it is not
          // yet present, in which case the inline cleanup below performs the removal instead.
          final EntryFuture previous = pendingSegmentRemovals.put(segmentId, entry);
          if (previous != null) {
            previous.cancel(false);
            log.warn("Replaced existing pending removal for segment[%s].", segmentId);
          }

          // Close the window where the timer fired before the put completed: the callback did
          // nothing in that case, so clean up inline under the same lock that the callback uses.
          if (entry.isDone()) {
            cleanupSelectorIfCurrent(segmentId, selector, entry);
          }

          log.debug(
              "Scheduled delayed cleanup of segment[%s] in [%d]ms. Waiting for a new server to announce it.",
              segmentId,
              delayMillis
          );
        } else {
          removeSegmentFromTimeline(segment, selector);
        }
      }
    }
  }

  /**
   * Removes a segment's timeline entry without firing {@code segmentRemoved} and without
   * dropping the selector from {@link #selectors}. Used by the delayed-drop path, where the
   * segment may be re-announced before the delay expires.
   */
  private void removeFromTimelineQuietly(final DataSegment segment, final ServerSelector selector)
  {
    final VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
    if (timeline != null) {
      timeline.remove(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
    }
  }

  /**
   * Body of the scheduled delayed-removal task. Runs on {@link #delayedRemovalExecutor}.
   *
   * @param scheduledAtNanos {@link System#nanoTime()} sampled when the task was scheduled; see
   *                         the ordering note in {@link #cleanupSelectorIfCurrent}.
   */
  private void runDelayedRemoval(
      final DataSegment segment,
      final ServerSelector selector,
      final EntryFuture entry,
      final long scheduledAtNanos
  )
  {
    cleanupSelectorIfCurrent(segment.getId(), selector, entry);
    // The delayed drop is now complete: the segment has no servers and is no longer on the
    // timeline, so release the metadata caches the same way immediate drops do. Without this,
    // pure drops leave stale segment metadata behind, since segmentRemoved is documented as
    // the authority for removal from the timeline.
    runTimelineCallbacks(callback -> callback.segmentRemoved(segment));
    log.debug(
        "Finished delayed cleanup of segment[%s] (scheduled at %d).",
        segment.getId(),
        scheduledAtNanos
    );
  }

  /**
   * Removes the selector from {@link #selectors} if, and only if, the given entry is still the
   * current pending removal for the segment, the selector is still the same empty instance, and
   * no server has re-announced the segment in the meantime. All three checks happen under
   * {@link #lock} so that a concurrent re-add cannot interleave between them.
   *
   * <p>Note on ordering: {@code lock} is also held by the scheduler thread while it publishes
   * this entry into {@link #pendingSegmentRemovals}. If the timer fires immediately, the callback
   * can therefore block here until that publication completes, after which the entry is present
   * and this method removes the selector. If it instead observes the entry as absent, the
   * scheduler thread will run this same cleanup inline once it sees {@code entry.isDone()}. Either
   * way exactly one of the two paths removes the selector.
   *
   * @return true if this call actually removed the selector
   */
  private boolean cleanupSelectorIfCurrent(
      final SegmentId segmentId,
      final ServerSelector selector,
      final EntryFuture entry
  )
  {
    synchronized (lock) {
      if (!pendingSegmentRemovals.remove(segmentId, entry)) {
        // A newer entry replaced us, or a re-announce already cancelled us.
        return false;
      }
      final ServerSelector currentSelector = selectors.get(segmentId);
      if (currentSelector != selector || !currentSelector.isEmpty()) {
        log.debug("Segment[%s] was re-announced during the drop delay. Keeping the selector.", segmentId);
        return false;
      }
      selectors.remove(segmentId);
      log.debug("Cleaned up selector for segment[%s] after delay.", segmentId);
      return true;
    }
  }

  /**
   * Handle for a scheduled delayed removal. The delegate future is assigned only after the
   * entry has been registered in {@link #pendingSegmentRemovals}, so the scheduled callback can
   * hold a reference to its own entry while the entry is still being constructed.
   *
   * <p>Also implements {@link ScheduledFuture} so it can be used directly as if it were the
   * underlying future; only {@link #cancel()} and {@link #isDone()} are ever called on it.
   */
  static final class EntryFuture implements ScheduledFuture<Object>
  {
    private volatile ScheduledFuture<?> delegate;

    void setDelayedRemovalFuture(final ScheduledFuture<?> future)
    {
      this.delegate = future;
    }

    @Override
    public boolean isDone()
    {
      final ScheduledFuture<?> future = delegate;
      return future == null || future.isDone();
    }

    @Override
    public boolean isCancelled()
    {
      final ScheduledFuture<?> future = delegate;
      return future != null && future.isCancelled();
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning)
    {
      final ScheduledFuture<?> future = delegate;
      return future == null || future.cancel(mayInterruptIfRunning);
    }

    @Override
    public long getDelay(final TimeUnit unit)
    {
      final ScheduledFuture<?> future = delegate;
      return future == null ? 0L : future.getDelay(unit);
    }

    @Override
    public int compareTo(final Delayed other)
    {
      final ScheduledFuture<?> future = delegate;
      return future == null ? 0 : future.compareTo(other);
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException
    {
      throw new UnsupportedOperationException("EntryFuture is never used as a task result.");
    }

    @Override
    public Object get(final long timeout, final TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException
    {
      throw new UnsupportedOperationException("EntryFuture is never used as a task result.");
    }
  }

  /**
   * Removes a segment from the broker's timeline when it has no remaining servers.
   * This is called when segment removal delay is 0 (immediate removal).
   * When delay is configured, the timeline entry is removed directly in
   * {@link #serverRemovedSegment} and the selector is cleaned up later by the
   * scheduled timer.
   */
  private void removeSegmentFromTimeline(final DataSegment segment, final ServerSelector selector)
  {
    final SegmentId segmentId = segment.getId();
    synchronized (lock) {
      // Double-check that the selector is still empty and present in the selectors map.
      // The selector might have been repopulated by a new server announcing the segment
      // during the delay period, or the segment might have been removed already.
      final ServerSelector currentSelector = selectors.get(segmentId);
      if (currentSelector == null || currentSelector != selector) {
        log.debug("Segment[%s] already removed from timeline or selector changed. Skipping removal.", segmentId);
        return;
      }
      if (!currentSelector.isEmpty()) {
        log.debug("Segment[%s] has servers again. Skipping removal.", segmentId);
        return;
      }

      VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
      selectors.remove(segmentId);

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
