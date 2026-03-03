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
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.broker.BrokerClientImpl;
import org.apache.druid.common.config.ConfigManager.SetResult;
import org.apache.druid.common.config.JacksonConfigManager;
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
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.security.AuthorizationUtils;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

@Path("/druid/coordinator/v1/broker/config")
@ResourceFilters(ConfigResourceFilter.class)
public class CoordinatorBrokerConfigsResource
{
  private static final Logger log = new Logger(CoordinatorBrokerConfigsResource.class);

  private final JacksonConfigManager configManager;
  private final AuditManager auditManager;
  private final AtomicReference<BrokerDynamicConfig> currentConfig;
  private final ServiceClientFactory clientFactory;
  private final ObjectMapper jsonMapper;
  private final DruidNodeDiscoveryProvider druidNodeDiscovery;
  private final ExecutorService exec;

  @Inject
  public CoordinatorBrokerConfigsResource(
      JacksonConfigManager configManager,
      AuditManager auditManager,
      @EscalatedGlobal ServiceClientFactory clientFactory,
      @Json ObjectMapper jsonMapper,
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider
  )
  {
    this.configManager = configManager;
    this.auditManager = auditManager;
    this.currentConfig = configManager.watch(
        BrokerDynamicConfig.CONFIG_KEY,
        BrokerDynamicConfig.class,
        new BrokerDynamicConfig(null)
    );
    this.clientFactory = clientFactory;
    this.jsonMapper = jsonMapper;
    this.druidNodeDiscovery = druidNodeDiscoveryProvider;
    this.exec = Execs.singleThreaded("CoordinatorBrokerConfigPush-%d");
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getBrokerDynamicConfig()
  {
    return Response.ok(currentConfig.get()).build();
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setBrokerDynamicConfig(
      BrokerDynamicConfig newConfig,
      @Context HttpServletRequest req
  )
  {
    try {
      final SetResult setResult = configManager.set(
          BrokerDynamicConfig.CONFIG_KEY,
          newConfig,
          AuthorizationUtils.buildAuditInfo(req)
      );

      if (setResult.isOk()) {
        // Push config to all brokers synchronously (with short timeout) for immediate effect
        pushConfigToBrokers(newConfig);
        return Response.ok().build();
      } else {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(setResult.getException()))
                       .build();
      }
    }
    catch (IllegalArgumentException e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ServletResourceUtils.sanitizeException(e))
                     .build();
    }
  }

  @GET
  @Path("/history")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getBrokerDynamicConfigHistory(
      @QueryParam("count") final Integer count
  )
  {
    if (count == null) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity("count parameter is required")
                     .build();
    }

    try {
      return Response.ok(
          auditManager.fetchAuditHistory(
              BrokerDynamicConfig.CONFIG_KEY,
              BrokerDynamicConfig.CONFIG_KEY,
              count
          )
      ).build();
    }
    catch (IllegalArgumentException e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ServletResourceUtils.sanitizeException(e))
                     .build();
    }
  }

  /**
   * Push the broker dynamic config to all currently known Brokers.
   */
  private void pushConfigToBrokers(BrokerDynamicConfig config)
  {
    Collection<DiscoveryDruidNode> brokers = getKnownBrokers();
    if (brokers.isEmpty()) {
      log.warn("No brokers discovered to push config to. Brokers will receive config via polling.");
      return;
    }

    log.info("Pushing broker config to [%d] brokers: [%s]", brokers.size(), config);

    int successCount = 0;
    int failCount = 0;

    for (DiscoveryDruidNode broker : brokers) {
      try {
        pushConfigToBroker(broker, config);
        successCount++;
      }
      catch (Exception e) {
        failCount++;
        log.error(e, "Failed to push config to broker [%s]", broker);
      }
    }

    log.info(
        "Broker config push complete: [%d] succeeded, [%d] failed out of [%d] total",
        successCount,
        failCount,
        brokers.size()
    );
  }

  /**
   * Push the broker dynamic config to a specific broker.
   */
  private void pushConfigToBroker(DiscoveryDruidNode broker, BrokerDynamicConfig config)
  {
    ServiceLocation brokerLocation = convertDiscoveryNodeToServiceLocation(broker);
    if (brokerLocation == null) {
      log.warn("Could not convert broker [%s] to service location", broker);
      return;
    }

    BrokerClient brokerClient = new BrokerClientImpl(
        clientFactory.makeClient(
            NodeRole.BROKER.getJsonName(),
            new FixedServiceLocator(brokerLocation),
            StandardRetryPolicy.builder().maxAttempts(1).build()  // Single attempt for speed
        ),
        jsonMapper
    );

    try {
      boolean success = brokerClient.updateBrokerDynamicConfig(config).get();
      if (success) {
        log.info("Successfully pushed broker config to [%s]", brokerLocation);
      } else {
        log.warn("Push returned false for broker [%s] - HTTP status was not OK", brokerLocation);
      }
    }
    catch (Exception e) {
      log.error(e, "Exception while pushing broker config to [%s]", brokerLocation);
      throw new RuntimeException("Failed to push to broker: " + brokerLocation, e);
    }
  }

  /**
   * Returns a collection of all brokers currently known to the druidNodeDiscovery.
   */
  private Collection<DiscoveryDruidNode> getKnownBrokers()
  {
    return druidNodeDiscovery.getForNodeRole(NodeRole.BROKER).getAllNodes();
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
}
