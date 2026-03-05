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

package org.apache.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.broker.BrokerClientImpl;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.rpc.FixedServiceLocator;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for syncing dynamic configuration to all brokers.
 * Subclasses must implement:
 * - {@link #getCurrentConfig()} to provide the latest config
 * - {@link #pushConfigToBroker(BrokerClient, Object)} to push config via the appropriate BrokerClient method
 * - {@link #getConfigTypeName()} for logging
 *
 * @param <T> the type of dynamic configuration (e.g., CoordinatorDynamicConfig, BrokerDynamicConfig)
 */
public abstract class BaseDynamicConfigSyncer<T>
{
  private static final Logger log = new Logger(BaseDynamicConfigSyncer.class);

  private final ObjectMapper jsonMapper;
  private final DruidNodeDiscoveryProvider druidNodeDiscovery;
  private final ServiceClientFactory clientFactory;
  private final ScheduledExecutorService exec;
  private final ServiceEmitter emitter;
  private @Nullable Future<?> syncFuture = null;

  @GuardedBy("this")
  private final Set<BrokerSyncStatus> inSyncBrokers;
  private final AtomicReference<T> lastKnownConfig = new AtomicReference<>();

  protected BaseDynamicConfigSyncer(
      final ServiceClientFactory clientFactory,
      final ObjectMapper jsonMapper,
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final ServiceEmitter emitter,
      final ScheduledExecutorService exec
  )
  {
    this.clientFactory = clientFactory;
    this.jsonMapper = jsonMapper;
    this.druidNodeDiscovery = druidNodeDiscoveryProvider;
    this.emitter = emitter;
    this.exec = exec;
    this.inSyncBrokers = ConcurrentHashMap.newKeySet();
  }

  /**
   * Get the current configuration to broadcast to brokers.
   */
  protected abstract T getCurrentConfig();

  /**
   * Push the config to a broker using the appropriate BrokerClient method.
   * @return true if the push was successful
   */
  protected abstract boolean pushConfigToBroker(BrokerClient brokerClient, T config) throws Exception;

  /**
   * Get the name of this config type for logging (e.g., "coordinator dynamic configuration", "broker dynamic configuration").
   */
  protected abstract String getConfigTypeName();

  /**
   * Queues the configuration sync to the brokers without blocking the calling thread.
   */
  public void queueBroadcastConfigToBrokers()
  {
    exec.submit(this::broadcastConfigToBrokers);
  }

  /**
   * Push the latest dynamic config to all currently known Brokers. Also
   * invalidates the set of inSyncBrokers if the config has changed.
   */
  @VisibleForTesting
  public void broadcastConfigToBrokers()
  {
    invalidateInSyncBrokersIfNeeded();
    final Stopwatch stopwatch = Stopwatch.createStarted();
    for (DiscoveryDruidNode broker : getKnownBrokers()) {
      pushConfigToBrokerNode(broker);
    }
    emitStat(
        Stats.Configuration.TOTAL_SYNC_TIME,
        RowKey.empty(),
        stopwatch.millisElapsed()
    );
  }

  /**
   * Returns the set of Brokers which have been updated with the latest config.
   */
  public synchronized Set<BrokerSyncStatus> getInSyncBrokers()
  {
    return Set.copyOf(inSyncBrokers);
  }

  /**
   * Schedules a periodic sync with brokers when the coordinator becomes the leader.
   */
  public void onLeaderStart()
  {
    log.info("Starting %s syncing to brokers on leader node.", getConfigTypeName());
    syncFuture = exec.scheduleAtFixedRate(
        this::broadcastConfigToBrokers,
        30L,
        60L,
        TimeUnit.SECONDS
    );
  }

  /**
   * Stops the sync when coordinator stops being the leader.
   */
  public void onLeaderStop()
  {
    log.info("Not leader, stopping %s syncing to brokers.", getConfigTypeName());
    if (syncFuture != null) {
      syncFuture.cancel(true);
    }
  }

  @LifecycleStop
  public void stop()
  {
    exec.shutdownNow();
  }

  /**
   * Push the latest dynamic config to the Broker at the brokerLocation param.
   */
  private void pushConfigToBrokerNode(DiscoveryDruidNode broker)
  {
    final Stopwatch stopwatch = Stopwatch.createStarted();
    final ServiceLocation brokerLocation = convertDiscoveryNodeToServiceLocation(broker);
    final BrokerClient brokerClient = new BrokerClientImpl(
        clientFactory.makeClient(
            NodeRole.BROKER.getJsonName(),
            new FixedServiceLocator(brokerLocation),
            StandardRetryPolicy.builder().maxAttempts(6).build()
        ),
        jsonMapper
    );

    try {
      T currentConfig = getCurrentConfig();
      boolean success = pushConfigToBroker(brokerClient, currentConfig);
      if (success) {
        markBrokerAsSynced(currentConfig, brokerLocation);
      }
    }
    catch (Exception e) {
      // Catch and ignore the exception, wait for the next sync.
      log.error(e, "Exception while syncing %s to broker[%s]", getConfigTypeName(), brokerLocation);
      emitStat(
          Stats.Configuration.BROKER_SYNC_ERROR,
          RowKey.with(Dimension.SERVER, broker.getDruidNode().getHostAndPortToUse()).build(),
          1
      );
    }
    emitStat(
        Stats.Configuration.BROKER_SYNC_TIME,
        RowKey.with(Dimension.SERVER, broker.getDruidNode().getHostAndPortToUse()).build(),
        stopwatch.millisElapsed()
    );
  }

  /**
   * Returns a collection of {@link DiscoveryDruidNode} for all brokers currently known to the druidNodeDiscovery.
   */
  private Collection<DiscoveryDruidNode> getKnownBrokers()
  {
    return druidNodeDiscovery.getForNodeRole(NodeRole.BROKER)
                             .getAllNodes();
  }

  /**
   * Clears the set of inSyncBrokers and updates the lastKnownConfig if the latest dynamic config is
   * different from the config tracked by this class.
   */
  private synchronized void invalidateInSyncBrokersIfNeeded()
  {
    final T currentConfig = getCurrentConfig();
    if (!currentConfig.equals(lastKnownConfig.get())) {
      // Config has changed, clear the inSync list.
      inSyncBrokers.clear();
      lastKnownConfig.set(currentConfig);
    }
  }

  /**
   * Adds a broker to the set of inSyncBrokers if the dynamic config has not changed.
   */
  private synchronized void markBrokerAsSynced(T config, ServiceLocation broker)
  {
    if (config.equals(lastKnownConfig.get())) {
      inSyncBrokers.add(new BrokerSyncStatus(broker, System.currentTimeMillis()));
    }
  }

  /**
   * Utility method to convert {@link DiscoveryDruidNode} to a {@link ServiceLocation}
   */
  @Nullable
  private static ServiceLocation convertDiscoveryNodeToServiceLocation(DiscoveryDruidNode discoveryDruidNode)
  {
    final DruidNode druidNode = discoveryDruidNode.getDruidNode();
    if (druidNode == null) {
      return null;
    }

    return new ServiceLocation(
        druidNode.getHost(),
        druidNode.getPlaintextPort(),
        druidNode.getTlsPort(),
        ""
    );
  }

  private void emitStat(CoordinatorStat stat, RowKey rowKey, long value)
  {
    ServiceMetricEvent.Builder eventBuilder = new ServiceMetricEvent.Builder();
    rowKey.getValues().forEach(
        (dim, dimValue) -> eventBuilder.setDimension(dim.reportedName(), dimValue)
    );
    emitter.emit(eventBuilder.setMetric(stat.getMetricName(), value));
  }
}
