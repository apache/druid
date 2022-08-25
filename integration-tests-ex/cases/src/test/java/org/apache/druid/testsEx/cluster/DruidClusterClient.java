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

package org.apache.druid.testsEx.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.DruidNode;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testsEx.config.ResolvedConfig;
import org.apache.druid.testsEx.config.ResolvedDruidService;
import org.apache.druid.testsEx.config.ResolvedService.ResolvedInstance;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

/**
 * Client to the Druid cluster described by the test cluster
 * configuration. Various clients exist for specific services or tasks:
 * this client is about the cluster as a whole, with operations used
 * by tests.
 */
public class DruidClusterClient
{
  private static final Logger log = new Logger(DruidClusterClient.class);

  private final ResolvedConfig config;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public DruidClusterClient(
      ResolvedConfig config,
      @TestClient HttpClient httpClient,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
  }

  public ResolvedConfig config()
  {
    return config;
  }

  /**
   * Return the configuration object for the leader for
   * the given service.
   */
  public ResolvedInstance leader(ResolvedDruidService service)
  {
    if (service.requireInstances().size() == 1) {
      return service.instance();
    }
    String leader = getLeader(service.service());
    return service.findHost(leader);
  }

  /**
   * Returns the leader URL for the given service.
   */
  public String getLeader(String service)
  {
    String url = StringUtils.format(
        "%s/druid/%s/v1/leader",
        config.routerUrl(),
        service
    );
    return get(url).getContent();
  }

  /**
   * Checks if a node is healthy, given the service and instance.
   *
   * @return `true` if the message returns `true`, `false` if the
   * message fails (indicating the node is not healthy.)
   */
  public boolean isHealthy(ResolvedDruidService service, ResolvedInstance instance)
  {
    return isHealthy(service.resolveUrl(instance));
  }

  /**
   * Checks if a node is healthy given the URL for that node.
   *
   * @return `true` if the message returns `true`, `false` if the
   * message fails (indicating the node is not healthy.)
   */
  public boolean isHealthy(String serviceUrl)
  {
    try {
      String url = StringUtils.format(
          "%s/status/health",
          serviceUrl
      );
      return getAs(url, Boolean.class);
    }
    catch (Exception e) {
      return false;
    }
  }

  /**
   * Returns the URL for the lead coordinator.
   */
  public String leadCoordinatorUrl()
  {
    ResolvedDruidService coord = config.requireCoordinator();
    ResolvedInstance leader = leader(coord);
    return coord.resolveUrl(leader);
  }

  /**
   * Calls the `/v1/cluster` endpoint on the lead coordinator.
   */
  public Map<String, Object> coordinatorCluster()
  {
    String url = StringUtils.format(
        "%s/druid/coordinator/v1/cluster",
        leadCoordinatorUrl()
    );
    return getAs(url, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
  }

  /**
   * Calls the `/v1/cluster` endpoint on the router.
   */
  public Map<String, Object> routerCluster()
  {
    String url = StringUtils.format(
        "%s/druid/router/v1/cluster",
        config.routerUrl()
    );
    return getAs(url, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
  }

  /**
   * Low-level HTTP get for the given URL.
   */
  public StatusResponseHolder get(String url)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(HttpMethod.GET, new URL(url)),
          StatusResponseHandler.getInstance()
      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error from GET [%s] status [%s] content [%s]",
            url,
            response.getStatus(),
            response.getContent()
        );
      }
      return response;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Issue a GET command and deserialize the JSON result to the given class.
   */
  public <T> T getAs(String url, Class<T> clazz)
  {
    StatusResponseHolder response = get(url);
    try {
      return jsonMapper.readValue(response.getContent(), clazz);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Issue a GET command and deserialize the JSON result to the given type reference.
   */
  public <T> T getAs(String url, TypeReference<T> typeRef)
  {
    StatusResponseHolder response = get(url);
    try {
      return jsonMapper.readValue(response.getContent(), typeRef);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Call the `/status/selfDiscovered` given a node URL.
   */
  public boolean selfDiscovered(String nodeUrl)
  {
    String url = StringUtils.format(
        "%s/status/selfDiscovered",
        nodeUrl
    );
    try {
      get(url);
    }
    catch (Exception e) {
      return false;
    }
    return true;
  }

  /**
   * Validates the cluster by waiting for each service declared in the
   * test configuration to report that it is healthy. By doing this at the
   * start of the test, individual tests don't have to retry to handle the
   * race condition that otherwise occurs between cluster and test startup.
   */
  public void validate()
  {
    log.info("Starting cluster validation");
    for (ResolvedDruidService service : config.requireDruid().values()) {
      for (ResolvedInstance instance : service.requireInstances()) {
        validateInstance(service, instance);
      }
    }
    log.info("Cluster validated.");
  }

  /**
   * Validate an instance by waiting for it to report that it is healthy.
   */
  private void validateInstance(ResolvedDruidService service, ResolvedInstance instance)
  {
    int timeoutMs = config.readyTimeoutSec() * 1000;
    int pollMs = config.readyPollMs();
    long startTime = System.currentTimeMillis();
    long updateTime = startTime + 5000;
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      if (isHealthy(service, instance)) {
        log.info(
            "Service %s, host %s is ready",
            service.service(),
            instance.clientHost());
        return;
      }
      long currentTime = System.currentTimeMillis();
      if (currentTime > updateTime) {
        log.info(
            "Service %s, host %s not ready, retrying",
            service.service(),
            instance.clientHost());
        updateTime = currentTime + 5000;
      }
      try {
        Thread.sleep(pollMs);
      }
      catch (InterruptedException e) {
        throw new RuntimeException("Interrupted during cluster validation");
      }
    }
    throw new RE(
        StringUtils.format("Service %s, instance %s not ready after %d ms.",
            service.service(),
            instance.tag() == null ? "<default>" : instance.tag(),
            timeoutMs));
  }

  /**
   * Wait for an instance to become ready given the URL and a description of
   * the service.
   */
  public void waitForNodeReady(String label, String url)
  {
    int timeoutMs = config.readyTimeoutSec() * 1000;
    int pollMs = config.readyPollMs();
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      if (isHealthy(url)) {
        log.info(
            "Service %s, url %s is ready",
            label,
            url);
        return;
      }
      log.info(
          "Service %s, url %s not ready, retrying",
          label,
          url);
      try {
        Thread.sleep(pollMs);
      }
      catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting for note to be ready");
      }
    }
    throw new RE(
        StringUtils.format("Service %s, url %s not ready after %d ms.",
            label,
            url,
            timeoutMs));
  }

  public String nodeUrl(DruidNode node)
  {
    return StringUtils.format(
        "http://%s:%s",
        config.hasProxy() ? config.proxyHost() : node.getHost(),
        node.getPlaintextPort()
    );
  }
}
