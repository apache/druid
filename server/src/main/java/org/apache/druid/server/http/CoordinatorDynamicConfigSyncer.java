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
import com.google.inject.Inject;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.broker.BrokerClientImpl;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.FixedServiceLocator;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Updates all brokers with the latest coordinator dynamic config.
 */
public class CoordinatorDynamicConfigSyncer
{
  private static final Logger log = new Logger(CoordinatorDynamicConfigSyncer.class);

  private final CoordinatorConfigManager configManager;
  private final ObjectMapper jsonMapper;
  private final DruidNodeDiscoveryProvider druidNodeDiscovery;

  private final AtomicReference<CoordinatorDynamicConfig> lastKnownConfig = new AtomicReference<>();
  private final ServiceClientFactory clientFactory;
  private final Set<BrokerSyncStatus> inSyncBrokers;
  private final ScheduledExecutorService exec;
  private @Nullable Future<?> syncFuture = null;

  @Inject
  public CoordinatorDynamicConfigSyncer(
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      final CoordinatorConfigManager configManager,
      @Json final ObjectMapper jsonMapper,
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider
  )
  {
    this.clientFactory = clientFactory;
    this.configManager = configManager;
    this.jsonMapper = jsonMapper;
    this.druidNodeDiscovery = druidNodeDiscoveryProvider;
    this.exec = Execs.scheduledSingleThreaded("CoordinatorDynamicConfigSyncer-%d");
    this.inSyncBrokers = ConcurrentHashMap.newKeySet();
  }

  public void triggerBroadcastConfigToBrokers()
  {
    exec.submit(this::broadcastConfigToBrokers);
  }

  public void broadcastConfigToBrokers()
  {
    invalidateInSyncBrokersIfNeeded();
    for (ServiceLocation broker : getKnownBrokers()) {
      pushConfigToBroker(broker);
    }
  }

  public synchronized ConfigSyncStatus getInSyncBrokers()
  {
    return new ConfigSyncStatus(Set.copyOf(inSyncBrokers));
  }

  public void onLeaderStart()
  {
    log.info("Starting coordinator config syncing to brokers on leader node.");
    syncFuture = exec.scheduleAtFixedRate(
        this::broadcastConfigToBrokers,
        30L,
        60L,
        TimeUnit.SECONDS
    );
  }

  public void onLeaderStop()
  {
    log.info("Not leader, stopping coordinator config syncing to brokers.");
    if (syncFuture != null) {
      syncFuture.cancel(true);
    }
  }

  private void pushConfigToBroker(ServiceLocation brokerLocation)
  {
    final BrokerClient brokerClient = new BrokerClientImpl(
        clientFactory.makeClient(
            NodeRole.BROKER.getJsonName(),
            new FixedServiceLocator(brokerLocation),
            StandardRetryPolicy.builder().maxAttempts(6).build()
        ),
        jsonMapper
    );

    try {
      CoordinatorDynamicConfig currentDynamicConfig = configManager.getCurrentDynamicConfig();
      boolean success = brokerClient.updateDynamicConfig(currentDynamicConfig).get();
      if (success) {
        markBrokerAsSynced(currentDynamicConfig, brokerLocation);
      }
    }
    catch (Exception e) {
      // Catch and ignore the exception, wait for the next sync.
      log.error(
          e,
          "Exception while syncing dynamic configuration to broker[%s]",
          brokerLocation
      );
    }
  }

  private Set<ServiceLocation> getKnownBrokers()
  {
    return druidNodeDiscovery.getForNodeRole(NodeRole.BROKER)
                             .getAllNodes()
                             .stream()
                             .map(CoordinatorDynamicConfigSyncer::convertDiscoveryNodeToServiceLocation)
                             .collect(Collectors.toSet());
  }

  private synchronized void invalidateInSyncBrokersIfNeeded()
  {
    final CoordinatorDynamicConfig currentDynamicConfig = configManager.getCurrentDynamicConfig();
    if (!currentDynamicConfig.equals(lastKnownConfig.get())) {
      // Config has changed, clear the inSync list.
      inSyncBrokers.clear();
      lastKnownConfig.set(currentDynamicConfig);
    }
  }

  private synchronized void markBrokerAsSynced(CoordinatorDynamicConfig config, ServiceLocation broker)
  {
    if (config.equals(lastKnownConfig.get())) {
      inSyncBrokers.add(new BrokerSyncStatus(broker, System.currentTimeMillis()));
    }
  }

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


}
