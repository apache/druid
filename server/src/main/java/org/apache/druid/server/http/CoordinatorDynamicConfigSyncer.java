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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.rpc.FixedServiceLocator;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Updates all brokers with the latest coordinator dynamic config.
 */
public class CoordinatorDynamicConfigSyncer
{
  private static final Logger log = new Logger(CoordinatorDynamicConfigSyncer.class);

  private final CoordinatorConfigManager configManager;
  private final ServerInventoryView serverInventoryView;
  private final ObjectMapper jsonMapper;

  private final AtomicReference<CoordinatorDynamicConfig> lastKnownConfig = new AtomicReference<>();
  private final ServiceClientFactory clientFactory;
  private final ExecutorService exec;
  private final Set<String> inSyncBrokers;

  @Inject
  public CoordinatorDynamicConfigSyncer(
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      final CoordinatorConfigManager configManager,
      @Json final ObjectMapper jsonMapper,
      final ServerInventoryView serverInventoryView
  )
  {
    this.clientFactory = clientFactory;
    this.configManager = configManager;
    this.jsonMapper = jsonMapper;
    this.serverInventoryView = serverInventoryView;
    this.exec = Execs.singleThreaded("DynamicConfigSyncer-%d");
    this.inSyncBrokers = ConcurrentHashMap.newKeySet();
  }

  public void broadcastConfigToBrokers()
  {
    invalidateInSyncBrokersIfNeeded();
    for (DruidServerMetadata broker : getKnownBrokers()) {
      exec.submit(() -> pushConfigToBroker(broker));
    }
  }

  public synchronized Set<String> getInSyncBrokers()
  {
    return Set.copyOf(inSyncBrokers);
  }

  private void pushConfigToBroker(DruidServerMetadata broker)
  {
    final ServiceLocation brokerLocation = ServiceLocation.fromDruidServerMetadata(broker);
    final ServiceClient brokerClient = clientFactory.makeClient(
        NodeRole.BROKER.getJsonName(),
        new FixedServiceLocator(brokerLocation),
        StandardRetryPolicy.builder().maxAttempts(6).build()
    );

    try {
      CoordinatorDynamicConfig currentDynamicConfig = configManager.getCurrentDynamicConfig();
      final RequestBuilder requestBuilder =
          new RequestBuilder(HttpMethod.POST, "/druid-internal/v1/dynamicConfiguration/coordinatorDynamicConfig")
              .jsonContent(jsonMapper, currentDynamicConfig);

      final BytesFullResponseHolder responseHolder = brokerClient.request(requestBuilder, new BytesFullResponseHandler());
      final HttpResponseStatus status = responseHolder.getStatus();
      if (status.equals(HttpResponseStatus.OK)) {
        addToInSyncBrokers(currentDynamicConfig, broker.getHost());
      } else {
        log.error(
            "Received status [%s] while posting dynamic configs to broker[%s]",
            status.getCode(),
            broker.getHostAndPort()
        );
      }
    }
    catch (Exception e) {
      // Catch and ignore the exception, wait for the next sync.
      log.error(
          e,
          "Exception while syncing dynamic configuration to broker[%s]",
          broker.getHostAndPort()
      );
    }
  }

  private Set<DruidServerMetadata> getKnownBrokers()
  {
    // TODO: Inventory view seems to be returning only an empty list, local work around for now.
    return ImmutableSet.of(
        new DruidServerMetadata(
            "localhost:8082",
            "localhost:8082",
            null, 0, ServerType.BROKER, "tier1", 0)
    );
  }

  private synchronized void invalidateInSyncBrokersIfNeeded()
  {
    final CoordinatorDynamicConfig currentDynamicConfig = configManager.getCurrentDynamicConfig();
    if (!currentDynamicConfig.equals(lastKnownConfig.get())) {
      // Config has changed, clear the inSync list.
      lastKnownConfig.set(currentDynamicConfig);
      inSyncBrokers.clear();
    }
  }

  private synchronized void addToInSyncBrokers(CoordinatorDynamicConfig config, String broker)
  {
    if (config.equals(lastKnownConfig.get())) {
      inSyncBrokers.add(broker);
    }
  }
}
