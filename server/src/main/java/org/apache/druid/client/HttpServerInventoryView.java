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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.ChangeRequestHttpSyncer;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.server.coordination.SegmentSchemasChangeRequest;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Duration;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class uses internal-discovery i.e. {@link DruidNodeDiscoveryProvider} to discover various queryable nodes in the
 * cluster such as historicals and realtime peon processes.
 *
 * For each queryable server, it uses HTTP GET /druid-internal/v1/segments (see docs for {@link
 * org.apache.druid.server.http.SegmentListerResource#getSegments}), to keep sync'd state of segments served by those
 * servers.
 */
public class HttpServerInventoryView implements ServerInventoryView, FilteredServerInventoryView
{
  public static final TypeReference<ChangeRequestsSnapshot<DataSegmentChangeRequest>> SEGMENT_LIST_RESP_TYPE_REF =
      new TypeReference<ChangeRequestsSnapshot<DataSegmentChangeRequest>>() {};

  private final EmittingLogger log = new EmittingLogger(HttpServerInventoryView.class);
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private final ConcurrentMap<ServerRemovedCallback, Executor> serverCallbacks = new ConcurrentHashMap<>();
  private final ConcurrentMap<SegmentCallback, Executor> segmentCallbacks = new ConcurrentHashMap<>();

  private final ConcurrentMap<SegmentCallback, Predicate<Pair<DruidServerMetadata, DataSegment>>> segmentPredicates =
      new ConcurrentHashMap<>();

  /**
   * Users of this instance can register filters for what segments should be stored and reported to registered
   * listeners. For example, A Broker node can be configured to keep state for segments of specific DataSource
   * by using this feature. In that way, Different Broker nodes can be used for dealing with Queries of Different
   * DataSources and not maintaining any segment information of other DataSources in memory.
   */
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter;
  private volatile Predicate<Pair<DruidServerMetadata, DataSegment>> finalPredicate;

  // For each queryable server, a name -> DruidServerHolder entry is kept
  private final ConcurrentHashMap<String, DruidServerHolder> servers = new ConcurrentHashMap<>();

  private final String execNamePrefix;
  private final ScheduledExecutorFactory executorFactory;
  private volatile ScheduledExecutorService inventorySyncExecutor;
  private volatile ScheduledExecutorService monitoringExecutor;

  private final HttpClient httpClient;
  private final ObjectMapper smileMapper;
  private final HttpServerInventoryViewConfig config;
  private final ServiceEmitter serviceEmitter;

  public HttpServerInventoryView(
      final ObjectMapper smileMapper,
      final HttpClient httpClient,
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter,
      final HttpServerInventoryViewConfig config,
      final ServiceEmitter serviceEmitter,
      final ScheduledExecutorFactory executorFactory,
      final String execNamePrefix
  )
  {
    this.httpClient = httpClient;
    this.smileMapper = smileMapper;
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.defaultFilter = defaultFilter;
    this.finalPredicate = defaultFilter;
    this.config = config;
    this.serviceEmitter = serviceEmitter;
    this.executorFactory = executorFactory;
    this.execNamePrefix = execNamePrefix;
  }


  @LifecycleStart
  public void start()
  {
    synchronized (lifecycleLock) {
      if (!lifecycleLock.canStart()) {
        throw new ISE("Could not start lifecycle");
      }

      log.info("Starting executor[%s].", execNamePrefix);

      try {
        inventorySyncExecutor = executorFactory.create(
            config.getNumThreads(),
            execNamePrefix + "-%s"
        );
        monitoringExecutor = executorFactory.create(1, execNamePrefix + "-monitor-%s");

        DruidNodeDiscovery druidNodeDiscovery = druidNodeDiscoveryProvider.getForService(DataNodeService.DISCOVERY_SERVICE_KEY);
        druidNodeDiscovery.registerListener(
            new DruidNodeDiscovery.Listener()
            {
              private final AtomicBoolean initialized = new AtomicBoolean(false);

              @Override
              public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
              {
                nodes.forEach(node -> serverAdded(toDruidServer(node)));
              }

              @Override
              public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
              {
                nodes.forEach(node -> serverRemoved(toDruidServer(node)));
              }

              @Override
              public void nodeViewInitialized()
              {
                if (!initialized.getAndSet(true)) {
                  inventorySyncExecutor.execute(HttpServerInventoryView.this::serverInventoryInitialized);
                }
              }

              private DruidServer toDruidServer(DiscoveryDruidNode node)
              {
                final DruidNode druidNode = node.getDruidNode();
                final DataNodeService dataNodeService = node.getService(DataNodeService.DISCOVERY_SERVICE_KEY, DataNodeService.class);
                if (dataNodeService == null) {
                  // this shouldn't typically happen, but just in case it does, make a dummy server to allow the
                  // callbacks to continue since serverAdded/serverRemoved only need node.getName()
                  return new DruidServer(
                      druidNode.getHostAndPortToUse(),
                      druidNode.getHostAndPort(),
                      druidNode.getHostAndTlsPort(),
                      0L,
                      ServerType.fromNodeRole(node.getNodeRole()),
                      DruidServer.DEFAULT_TIER,
                      DruidServer.DEFAULT_PRIORITY
                  );
                }
                return new DruidServer(
                    druidNode.getHostAndPortToUse(),
                    druidNode.getHostAndPort(),
                    druidNode.getHostAndTlsPort(),
                    dataNodeService.getMaxSize(),
                    dataNodeService.getServerType(),
                    dataNodeService.getTier(),
                    dataNodeService.getPriority()
                );
              }
            }
        );

        ScheduledExecutors.scheduleWithFixedDelay(
            monitoringExecutor,
            Duration.standardSeconds(60),
            Duration.standardMinutes(5),
            this::checkAndResetUnhealthyServers
        );
        ScheduledExecutors.scheduleAtFixedRate(
            monitoringExecutor,
            Duration.standardSeconds(30),
            Duration.standardMinutes(1),
            this::emitServerStatusMetrics
        );

        lifecycleLock.started();
      }
      finally {
        lifecycleLock.exitStart();
      }

      log.info("Started executor[%s].", execNamePrefix);
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lifecycleLock) {
      if (!lifecycleLock.canStop()) {
        throw new ISE("can't stop.");
      }

      log.info("Stopping executor[%s].", execNamePrefix);

      if (inventorySyncExecutor != null) {
        inventorySyncExecutor.shutdownNow();
      }
      if (monitoringExecutor != null) {
        monitoringExecutor.shutdownNow();
      }

      log.info("Stopped executor[%s].", execNamePrefix);
    }
  }

  @Override
  public void registerSegmentCallback(
      Executor exec,
      SegmentCallback callback,
      Predicate<Pair<DruidServerMetadata, DataSegment>> filter
  )
  {
    if (lifecycleLock.isStarted()) {
      throw new ISE("Lifecycle has already started.");
    }

    SegmentCallback filteringSegmentCallback = new FilteringSegmentCallback(callback, filter);
    segmentCallbacks.put(filteringSegmentCallback, exec);
    segmentPredicates.put(filteringSegmentCallback, filter);

    updateFinalPredicate();
  }

  @Override
  public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
  {
    if (lifecycleLock.isStarted()) {
      throw new ISE("Lifecycle has already started.");
    }

    serverCallbacks.put(callback, exec);
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    if (lifecycleLock.isStarted()) {
      throw new ISE("Lifecycle has already started.");
    }

    segmentCallbacks.put(callback, exec);
  }

  @Override
  public DruidServer getInventoryValue(String containerKey)
  {
    DruidServerHolder holder = servers.get(containerKey);
    if (holder != null) {
      return holder.druidServer;
    }
    return null;
  }

  @Override
  public Collection<DruidServer> getInventory()
  {
    // Returning a lazy collection, because currently getInventory() is always used for one-time iteration. It's OK for
    // storing in a field and repetitive iteration as well, because the lambda is very cheap - just a final field
    // access.
    return Collections2.transform(servers.values(), serverHolder -> serverHolder.druidServer);
  }

  private void runSegmentCallbacks(
      final Function<SegmentCallback, CallbackAction> fn
  )
  {
    for (final Map.Entry<SegmentCallback, Executor> entry : segmentCallbacks.entrySet()) {
      entry.getValue().execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (CallbackAction.UNREGISTER == fn.apply(entry.getKey())) {
                segmentCallbacks.remove(entry.getKey());
                if (segmentPredicates.remove(entry.getKey()) != null) {
                  updateFinalPredicate();
                }
              }
            }
          }
      );
    }
  }

  private void runServerRemovedCallbacks(final DruidServer server)
  {
    for (final Map.Entry<ServerRemovedCallback, Executor> entry : serverCallbacks.entrySet()) {
      entry.getValue().execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (CallbackAction.UNREGISTER == entry.getKey().serverRemoved(server)) {
                serverCallbacks.remove(entry.getKey());
              }
            }
          }
      );
    }
  }

  /**
   * Waits until the sync wait timeout for all servers to be synced at least once.
   * Finally calls {@link SegmentCallback#segmentViewInitialized()} regardless of
   * whether all servers synced successfully or not.
   */
  private void serverInventoryInitialized()
  {
    long start = System.currentTimeMillis();
    long serverSyncWaitTimeout = config.getServerTimeout() + 2 * ChangeRequestHttpSyncer.HTTP_TIMEOUT_EXTRA_MS;

    List<DruidServerHolder> uninitializedServers = new ArrayList<>();
    for (DruidServerHolder server : servers.values()) {
      if (!server.isSyncedSuccessfullyAtleastOnce()) {
        uninitializedServers.add(server);
      }
    }

    while (!uninitializedServers.isEmpty() && ((System.currentTimeMillis() - start) < serverSyncWaitTimeout)) {
      try {
        Thread.sleep(5000);
      }
      catch (InterruptedException ex) {
        throw new RE(ex, "Interrupted while waiting for queryable server initial successful sync.");
      }

      log.info("Waiting for [%d] servers to sync successfully.", uninitializedServers.size());
      uninitializedServers.removeIf(
          serverHolder -> serverHolder.isSyncedSuccessfullyAtleastOnce()
                          || serverHolder.isStopped()
      );
    }

    if (uninitializedServers.isEmpty()) {
      log.info("All servers have been synced successfully at least once.");
    } else {
      for (DruidServerHolder server : uninitializedServers) {
        log.warn(
            "Server[%s] might not yet be synced successfully. We will continue to retry that in the background.",
            server.druidServer.getName()
        );
      }
    }

    log.info("Invoking segment view initialized callbacks.");
    runSegmentCallbacks(SegmentCallback::segmentViewInitialized);
  }

  private void updateFinalPredicate()
  {
    finalPredicate = Predicates.or(defaultFilter, Predicates.or(segmentPredicates.values()));
  }

  @VisibleForTesting
  void serverAdded(DruidServer server)
  {
    synchronized (servers) {
      DruidServerHolder holder = servers.get(server.getName());
      if (holder == null) {
        log.info("Server[%s] appeared.", server.getName());
        holder = new DruidServerHolder(server);
        servers.put(server.getName(), holder);
        holder.start();
      } else {
        log.info("Server[%s] already exists.", server.getName());
      }
    }
  }

  private void serverRemoved(DruidServer server)
  {
    synchronized (servers) {
      DruidServerHolder holder = servers.remove(server.getName());
      if (holder != null) {
        log.info("Server[%s] disappeared.", server.getName());
        holder.stop();
        runServerRemovedCallbacks(holder.druidServer);
      } else {
        log.info("Ignoring remove notification for unknown server[%s].", server.getName());
      }
    }
  }

  /**
   * This method returns the debugging information exposed by {@link HttpServerInventoryViewResource} and meant
   * for that use only. It must not be used for any other purpose.
   */
  public Map<String, Object> getDebugInfo()
  {
    Preconditions.checkArgument(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    Map<String, Object> result = Maps.newHashMapWithExpectedSize(servers.size());
    for (Map.Entry<String, DruidServerHolder> e : servers.entrySet()) {
      DruidServerHolder serverHolder = e.getValue();
      result.put(
          e.getKey(),
          serverHolder.syncer.getDebugInfo()
      );
    }
    return result;
  }

  @VisibleForTesting
  void checkAndResetUnhealthyServers()
  {
    // Ensure that the collection is not being modified during iteration. Iterate over a copy
    final Set<Map.Entry<String, DruidServerHolder>> serverEntrySet = ImmutableSet.copyOf(servers.entrySet());
    for (Map.Entry<String, DruidServerHolder> e : serverEntrySet) {
      DruidServerHolder serverHolder = e.getValue();
      if (serverHolder.syncer.needsReset()) {
        synchronized (servers) {
          // Reset only if the server is still present in the map
          if (servers.containsKey(e.getKey())) {
            log.warn(
                "Resetting server[%s] with state[%s] as it is not syncing properly.",
                serverHolder.druidServer.getName(),
                serverHolder.syncer.getDebugInfo()
            );
            serverRemoved(serverHolder.druidServer);
            serverAdded(serverHolder.druidServer.copyWithoutSegments());
          }
        }
      }
    }
  }

  private void emitServerStatusMetrics()
  {
    final ServiceMetricEvent.Builder eventBuilder = ServiceMetricEvent.builder();
    try {
      final Map<String, DruidServerHolder> serversCopy = ImmutableMap.copyOf(servers);
      serversCopy.forEach((serverName, serverHolder) -> {
        final DruidServer server = serverHolder.druidServer;
        eventBuilder.setDimension("tier", server.getTier());
        eventBuilder.setDimension("server", serverName);

        final boolean isSynced = serverHolder.syncer.isSyncedSuccessfully();
        serviceEmitter.emit(
            eventBuilder.setMetric("serverview/sync/healthy", isSynced ? 1 : 0)
        );
        final long unstableTimeMillis = serverHolder.syncer.getUnstableTimeMillis();
        if (unstableTimeMillis > 0) {
          serviceEmitter.emit(
              eventBuilder.setMetric("serverview/sync/unstableTime", unstableTimeMillis)
          );
        }
      });
    }
    catch (Exception e) {
      log.error(e, "Error while emitting server status metrics");
    }
  }

  @Override
  public boolean isStarted()
  {
    return lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
  {
    DruidServerHolder holder = servers.get(serverKey);
    return holder != null && holder.druidServer.getSegment(segment.getId()) != null;
  }

  private class DruidServerHolder
  {
    private final DruidServer druidServer;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private final ChangeRequestHttpSyncer<DataSegmentChangeRequest> syncer;

    DruidServerHolder(DruidServer druidServer)
    {
      this.druidServer = druidServer;

      try {
        HostAndPort hostAndPort = HostAndPort.fromString(druidServer.getHost());
        this.syncer = new ChangeRequestHttpSyncer<>(
            smileMapper,
            httpClient,
            inventorySyncExecutor,
            new URL(druidServer.getScheme(), hostAndPort.getHost(), hostAndPort.getPort(), "/"),
            "/druid-internal/v1/segments",
            SEGMENT_LIST_RESP_TYPE_REF,
            config.getServerTimeout(),
            config.getServerUnstabilityTimeout(),
            createSyncListener()
        );
      }
      catch (MalformedURLException ex) {
        throw new IAE(ex, "Failed to construct server URL.");
      }
    }

    void start()
    {
      syncer.start();
    }

    void stop()
    {
      syncer.stop();
      stopped.set(true);
    }

    boolean isStopped()
    {
      return stopped.get();
    }

    boolean isSyncedSuccessfullyAtleastOnce()
    {
      return syncer.isInitialized();
    }

    private ChangeRequestHttpSyncer.Listener<DataSegmentChangeRequest> createSyncListener()
    {
      return new ChangeRequestHttpSyncer.Listener<DataSegmentChangeRequest>()
      {
        @Override
        public void fullSync(List<DataSegmentChangeRequest> changes)
        {
          Map<SegmentId, DataSegment> toRemove = Maps.newHashMapWithExpectedSize(druidServer.getTotalSegments());
          druidServer.iterateAllSegments().forEach(segment -> toRemove.put(segment.getId(), segment));

          for (DataSegmentChangeRequest request : changes) {
            if (request instanceof SegmentChangeRequestLoad) {
              DataSegment segment = ((SegmentChangeRequestLoad) request).getSegment();
              toRemove.remove(segment.getId());
              addSegment(segment, true);
            } else if (request instanceof SegmentSchemasChangeRequest) {
              runSegmentCallbacks(input -> input.segmentSchemasAnnounced(((SegmentSchemasChangeRequest) request).getSegmentSchemas()));
            } else {
              log.error(
                  "Server[%s] gave a non-load dataSegmentChangeRequest[%s]., Ignored.",
                  druidServer.getName(),
                  request
              );
            }
          }

          for (DataSegment segmentToRemove : toRemove.values()) {
            removeSegment(segmentToRemove, true);
          }
        }

        @Override
        public void deltaSync(List<DataSegmentChangeRequest> changes)
        {
          for (DataSegmentChangeRequest request : changes) {
            if (request instanceof SegmentChangeRequestLoad) {
              addSegment(((SegmentChangeRequestLoad) request).getSegment(), false);
            } else if (request instanceof SegmentChangeRequestDrop) {
              removeSegment(((SegmentChangeRequestDrop) request).getSegment(), false);
            } else if (request instanceof SegmentSchemasChangeRequest) {
              runSegmentCallbacks(input -> input.segmentSchemasAnnounced(((SegmentSchemasChangeRequest) request).getSegmentSchemas()));
            } else {
              log.error(
                  "Server[%s] gave a non load/drop dataSegmentChangeRequest[%s], Ignored.",
                  druidServer.getName(),
                  request
              );
            }
          }
        }
      };
    }

    private void addSegment(DataSegment segment, boolean fullSync)
    {
      if (finalPredicate.apply(Pair.of(druidServer.getMetadata(), segment))) {
        if (druidServer.getSegment(segment.getId()) == null) {
          DataSegment theSegment = DataSegmentInterner.intern(segment);
          druidServer.addDataSegment(theSegment);
          runSegmentCallbacks(
              new Function<SegmentCallback, CallbackAction>()
              {
                @Override
                public CallbackAction apply(SegmentCallback input)
                {
                  return input.segmentAdded(druidServer.getMetadata(), theSegment);
                }
              }
          );
        } else if (!fullSync) {
          // duplicates can happen when doing a full sync from a 'reset', so only warn for dupes on delta changes
          log.warn(
              "Not adding or running callbacks for existing segment[%s] on server[%s]",
              segment.getId(),
              druidServer.getName()
          );
        }
      }
    }

    private void removeSegment(final DataSegment segment, boolean fullSync)
    {
      if (druidServer.removeDataSegment(segment.getId()) != null) {
        runSegmentCallbacks(
            new Function<SegmentCallback, CallbackAction>()
            {
              @Override
              public CallbackAction apply(SegmentCallback input)
              {
                return input.segmentRemoved(druidServer.getMetadata(), segment);
              }
            }
        );
      } else if (!fullSync) {
        // duplicates can happen when doing a full sync from a 'reset', so only warn for dupes on delta changes
        log.warn(
            "Not running cleanup or callbacks for non-existing segment[%s] on server[%s]",
            segment.getId(),
            druidServer.getName()
        );
      }
    }
  }
}
