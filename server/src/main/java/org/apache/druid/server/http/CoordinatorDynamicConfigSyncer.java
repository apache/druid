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
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.StringUtils;
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
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.URL;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
  private final ExecutorService exec;
  private final Set<String> inSyncBrokers;

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
    this.exec = Execs.singleThreaded("DynamicConfigSyncer-%d");
    this.inSyncBrokers = ConcurrentHashMap.newKeySet();
  }

  public void broadcastConfigToBrokers()
  {
    invalidateInSyncBrokersIfNeeded();
    for (ServiceLocation broker : getKnownBrokers()) {
      exec.submit(() -> pushConfigToBroker(broker));
    }
  }

  public synchronized Set<String> getInSyncBrokers()
  {
    return Set.copyOf(inSyncBrokers);
  }

  private void pushConfigToBroker(ServiceLocation brokerLocation)
  {
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
        addToInSyncBrokers(currentDynamicConfig, brokerLocation);
      } else {
        log.error(
            "Received status [%s] while posting dynamic configs to broker[%s]",
            status.getCode(),
            brokerLocation
        );
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
                             .map(DiscoveryDruidNode::toServiceLocation)
                             .collect(Collectors.toSet());
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

  private synchronized void addToInSyncBrokers(CoordinatorDynamicConfig config, ServiceLocation broker)
  {
    final URL url = broker.toURL("");
    if (config.equals(lastKnownConfig.get())) {
      inSyncBrokers.add(StringUtils.format("%s:%s", url.getHost(), url.getPort()));
    }
  }
}
