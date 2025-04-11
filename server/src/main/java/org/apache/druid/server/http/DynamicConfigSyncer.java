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
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ServerInventoryView;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.druid.discovery.NodeRole.BROKER;

public class DynamicConfigSyncer
{
  private static final Logger log = new Logger(DynamicConfigSyncer.class);
  protected final ExecutorService exec;
  private final ServiceClientFactory clientFactory;
  private final CoordinatorConfigManager configManager;
  private final List<String> inSyncBrokers;
  private final ObjectMapper objectMapper;
  private final ServerInventoryView serverInventoryView;
  private final AtomicReference<CoordinatorDynamicConfig> lastKnownConfig = new AtomicReference<>();

  @Inject
  public DynamicConfigSyncer(
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      CoordinatorConfigManager configManager,
      @Json ObjectMapper objectMapper,
      ServerInventoryView serverInventoryView
      )
  {
    this.clientFactory = clientFactory;
    this.configManager = configManager;
    this.objectMapper = objectMapper;
    this.serverInventoryView = serverInventoryView;
    this.exec = Execs.singleThreaded("DynamicConfigSyncer-%d");
    this.inSyncBrokers = new ArrayList<>();
  }

  private void updateBroker(DruidServerMetadata broker)
  {
    ServiceLocation serviceLocation = ServiceLocation.fromDruidServerMetadata(broker);
    ServiceClient serviceClient = clientFactory.makeClient(
        BROKER.getJsonName(),
        new FixedServiceLocator(serviceLocation),
        StandardRetryPolicy.builder().maxAttempts(6).build()
    );

    try {
      RequestBuilder requestBuilder =
          new RequestBuilder(HttpMethod.POST, "/druid-internal/v1/dynamicConfiguration/coordinatorDynamicConfig")
              .jsonContent(objectMapper, configManager.getCurrentDynamicConfig());

      BytesFullResponseHolder request = serviceClient.request(
          requestBuilder,
          new BytesFullResponseHandler()
      );
      HttpResponseStatus status = request.getStatus();
      if (status.equals(HttpResponseStatus.OK)) {
        log.info("Successfully posted dynamic configs to [%s]", broker.getHost());
        inSyncBrokers.add(broker.getHost());
        log.info("In sync: [%s]", inSyncBrokers);
      } else {
        log.error("Error [%s] while posting dynamic configs to [%s]", status.getCode(), broker.getHostAndPort());
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void updateConfigIfNeeded()
  {
    log.info("Running Broker sync duty");
    CoordinatorDynamicConfig currentDynamicConfig = configManager.getCurrentDynamicConfig();
    if (currentDynamicConfig.equals(lastKnownConfig.get())) {
      log.info("No need to change, already latest");
    } else {
      lastKnownConfig.set(currentDynamicConfig);
      inSyncBrokers.clear();
      getCurrentBrokers().forEach(broker -> exec.submit(() -> updateBroker(broker)));
    }
  }

  public Set<DruidServerMetadata> getCurrentBrokers()
  {
    // TODO: work around
    Set<DruidServerMetadata> collect = serverInventoryView
        .getInventory()
        .stream()
        .filter(ds -> ServerType.BROKER.equals(ds.getType()))
        .map(DruidServer::getMetadata)
        .collect(Collectors.toSet());
    log.info("SERVER VIEW BROKERS: [%s]", collect);
    return ImmutableSet.of(
                            new DruidServerMetadata(
                                "localhost:8082",
                                "localhost:8082",
                                null, 0, ServerType.BROKER, "tier1", 0)
    );
  }

  public List<String> getInSyncBrokers()
  {
    return inSyncBrokers;
  }
}
