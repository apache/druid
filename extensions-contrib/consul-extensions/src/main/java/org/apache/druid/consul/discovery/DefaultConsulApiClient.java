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

package org.apache.druid.consul.discovery;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.health.HealthServicesRequest;
import com.ecwid.consul.v1.health.model.HealthService;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of {@link ConsulApiClient} using the Ecwid Consul client library.
 */
public class DefaultConsulApiClient implements ConsulApiClient
{
  private static final Logger LOGGER = new Logger(DefaultConsulApiClient.class);

  // Consul service metadata has a limit of 512 bytes per value
  // Use a safe limit to avoid edge cases (450 bytes leaves room for key name overhead)
  private static final int MAX_METADATA_VALUE_SIZE_BYTES = 450;
  private static final long MIN_SESSION_TTL_SECONDS = 30;
  private static final long MIN_HEALTH_CHECK_INTERVAL_SECONDS = 1;

  private final ConsulClient consulClient;
  private final ConsulDiscoveryConfig config;
  private final ObjectMapper jsonMapper;

  public DefaultConsulApiClient(
      ConsulClient consulClient,
      ConsulDiscoveryConfig config,
      ObjectMapper jsonMapper
  )
  {
    this.consulClient = Preconditions.checkNotNull(consulClient, "consulClient");
    this.config = Preconditions.checkNotNull(config, "config");
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");

    LOGGER.info(
        "Created DefaultConsulApiClient for [%s:%d] with service prefix [%s]",
        config.getConnection().getHost(),
        config.getConnection().getPort(),
        config.getService().getServicePrefix()
    );
  }

  @Override
  public void registerService(DiscoveryDruidNode node) throws Exception
  {
    String serviceId = ConsulServiceIds.serviceId(config, node);
    String serviceName = ConsulServiceIds.serviceName(config, node.getNodeRole());

    NewService service = new NewService();
    service.setId(serviceId);
    service.setName(serviceName);
    service.setAddress(node.getDruidNode().getHost());
    service.setPort(node.getDruidNode().getPortToUse());

    List<String> tags = new ArrayList<>();
    tags.add("druid");
    tags.add("role:" + node.getNodeRole().getJsonName());
    if (config.getService().getServiceTags() != null) {
      for (Map.Entry<String, String> e : config.getService().getServiceTags().entrySet()) {
        if (e.getKey() != null && e.getValue() != null) {
          tags.add(e.getKey() + ":" + e.getValue());
        }
      }
    }
    service.setTags(tags);

    // Serialize the full DiscoveryDruidNode as metadata
    String nodeJson = jsonMapper.writeValueAsString(node);

    // Consul service metadata has a 512 byte limit per value
    // If the JSON is too large, store it in Consul KV and reference it from metadata
    Map<String, String> meta = new HashMap<>();
    int nodeJsonBytes = nodeJson.getBytes(StandardCharsets.UTF_8).length;
    if (nodeJsonBytes <= MAX_METADATA_VALUE_SIZE_BYTES) {
      // Small enough - store directly in metadata
      meta.put("druid_node", nodeJson);
    } else {
      // Too large - store in KV and reference it
      String kvKey = ConsulServiceIds.nodeKvKey(config, serviceId);
      PutParams putParams = new PutParams();  // No session locking for simple storage
      consulClient.setKVValue(kvKey, nodeJson, config.getAuth().getAclToken(), putParams, buildQueryParams());
      meta.put("druid_node_kv", kvKey);
      LOGGER.debug(
          "Node metadata for [%s] is too large (%d bytes), stored in KV at [%s]",
          serviceId,
          nodeJsonBytes,
          kvKey
      );
    }
    service.setMeta(meta);

    NewService.Check check = new NewService.Check();
    long intervalSeconds = Math.max(MIN_HEALTH_CHECK_INTERVAL_SECONDS, config.getService().getHealthCheckInterval().getStandardSeconds());
    long ttlSeconds = Math.max(MIN_SESSION_TTL_SECONDS, intervalSeconds * 3);
    check.setTtl(StringUtils.format("%ds", ttlSeconds));
    check.setDeregisterCriticalServiceAfter(
        StringUtils.format("%ds", config.getService().getDeregisterAfter().getStandardSeconds())
    );
    service.setCheck(check);

    consulClient.agentServiceRegister(service, config.getAuth().getAclToken());
    LOGGER.info("Registered service [%s] with Consul", serviceId);

    try {
      consulClient.agentCheckPass("service:" + serviceId, "Druid node is healthy", config.getAuth().getAclToken());
    }
    catch (Exception e) {
      // Log but don't fail - the periodic health check will eventually mark it as passing
      LOGGER.warn(e, "Failed to immediately mark service [%s] as healthy, will retry via periodic health check", serviceId);
    }
  }

  @Override
  @SuppressWarnings("RedundantThrows")
  public void deregisterService(String serviceId) throws Exception
  {
    try {
      String kvKey = ConsulServiceIds.nodeKvKey(config, serviceId);
      consulClient.deleteKVValue(kvKey, config.getAuth().getAclToken());
    }
    catch (Exception e) {
      LOGGER.debug(e, "Failed to delete KV entry for service [%s] during deregistration", serviceId);
    }

    consulClient.agentServiceDeregister(serviceId, config.getAuth().getAclToken());
    LOGGER.info("Deregistered service [%s] from Consul", serviceId);
  }

  @Override
  @SuppressWarnings("RedundantThrows")
  public void passTtlCheck(String serviceId, String note) throws Exception
  {
    consulClient.agentCheckPass("service:" + serviceId, note, config.getAuth().getAclToken());
  }

  @Override
  @SuppressWarnings("RedundantThrows")
  public List<DiscoveryDruidNode> getHealthyServices(NodeRole nodeRole) throws Exception
  {
    String serviceName = makeServiceName(nodeRole);
    HealthServicesRequest request = HealthServicesRequest.newBuilder()
        .setPassing(true)
        .setQueryParams(buildQueryParams())
        .setToken(config.getAuth().getAclToken())
        .build();
    Response<List<HealthService>> response = consulClient.getHealthServices(serviceName, request);

    return parseHealthServices(response.getValue());
  }

  @Override
  @SuppressWarnings("RedundantThrows")
  public ConsulWatchResult watchServices(NodeRole nodeRole, long lastIndex, long waitSeconds) throws Exception
  {
    String serviceName = makeServiceName(nodeRole);

    HealthServicesRequest request = HealthServicesRequest.newBuilder()
        .setPassing(true)
        .setQueryParams(buildQueryParams(waitSeconds, lastIndex))
        .setToken(config.getAuth().getAclToken())
        .build();
    Response<List<HealthService>> response = consulClient.getHealthServices(serviceName, request);

    List<DiscoveryDruidNode> nodes = parseHealthServices(response.getValue());
    long newIndex = response.getConsulIndex() != null ? response.getConsulIndex() : lastIndex;

    return new ConsulWatchResult(nodes, newIndex);
  }

  private QueryParams buildQueryParams()
  {
    if (config.getService().getDatacenter() != null) {
      return new QueryParams(config.getService().getDatacenter());
    }
    return QueryParams.DEFAULT;
  }

  private QueryParams buildQueryParams(long waitSeconds, long index)
  {
    if (config.getService().getDatacenter() != null) {
      return new QueryParams(config.getService().getDatacenter(), waitSeconds, index);
    }
    return new QueryParams(waitSeconds, index);
  }

  @Override
  public void close()
  {
    // Consul client doesn't need explicit cleanup
    LOGGER.info("Closed Consul client");
  }

  private String makeServiceName(NodeRole nodeRole)
  {
    return ConsulServiceIds.serviceName(config, nodeRole);
  }

  private List<DiscoveryDruidNode> parseHealthServices(List<HealthService> healthServices)
  {
    if (healthServices == null || healthServices.isEmpty()) {
      return Collections.emptyList();
    }

    List<DiscoveryDruidNode> nodes = new ArrayList<>();
    for (HealthService healthService : healthServices) {
      try {
        if (healthService.getService() == null ||
            healthService.getService().getMeta() == null) {
          continue;
        }

        Map<String, String> meta = healthService.getService().getMeta();
        final String nodeJson;

        if (meta.containsKey("druid_node")) {
          nodeJson = meta.get("druid_node");
        } else if (meta.containsKey("druid_node_kv")) {
          String kvKey = meta.get("druid_node_kv");
          Response<GetValue> kvResponse = consulClient.getKVValue(kvKey, config.getAuth().getAclToken(), buildQueryParams());
          if (kvResponse != null && kvResponse.getValue() != null && kvResponse.getValue().getValue() != null) {
            nodeJson = new String(
                Base64.getDecoder().decode(kvResponse.getValue().getValue()),
                StandardCharsets.UTF_8
            );
          } else {
            LOGGER.warn("KV entry [%s] not found for service metadata", kvKey);
            continue;
          }
        } else {
          continue;
        }

        DiscoveryDruidNode node = jsonMapper.readValue(nodeJson, DiscoveryDruidNode.class);
        nodes.add(node);
      }
      catch (IOException e) {
        LOGGER.error(e, "Failed to parse DiscoveryDruidNode from Consul service metadata");
      }
      catch (Exception e) {
        LOGGER.error(e, "Failed to retrieve or parse DiscoveryDruidNode from Consul");
      }
    }

    return nodes;
  }
}
