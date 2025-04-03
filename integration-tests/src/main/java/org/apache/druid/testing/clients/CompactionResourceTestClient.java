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

package org.apache.druid.testing.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.http.CompactionConfigsResponse;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.compaction.CompactionSimulateResult;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.compaction.CompactionTriggerResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class CompactionResourceTestClient
{
  private static final Logger log = new Logger(CompactionResourceTestClient.class);

  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String coordinator;
  private final String overlord;
  private final StatusResponseHandler responseHandler;

  @Inject
  CompactionResourceTestClient(
      ObjectMapper jsonMapper,
      @TestClient HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.coordinator = config.getCoordinatorUrl();
    this.overlord = config.getOverlordUrl();
    this.responseHandler = StatusResponseHandler.getInstance();
  }

  private String getCoordinatorURL()
  {
    return StringUtils.format(
        "%s/druid/coordinator/v1/",
        coordinator
    );
  }

  private String getOverlordURL()
  {
    return StringUtils.format("%s/druid/indexer/v1", overlord);
  }

  public void submitCompactionConfig(final DataSourceCompactionConfig dataSourceCompactionConfig) throws Exception
  {
    final String dataSource = dataSourceCompactionConfig.getDataSource();
    String url = StringUtils.format(
        "%s/compaction/config/datasources/%s",
        getOverlordURL(), StringUtils.urlEncode(dataSource)
    );
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.POST, new URL(url)).setContent(
            "application/json",
            jsonMapper.writeValueAsBytes(dataSourceCompactionConfig)
        ),
        responseHandler
    ).get();

    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while submiting compaction config status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
    log.info(
        "Submitted compaction config for datasource[%s] with response[%s]",
        dataSource, response.getContent()
    );
  }

  public void deleteDataSourceCompactionConfig(final String dataSource) throws Exception
  {
    String url = StringUtils.format(
        "%s/compaction/config/datasources/%s",
        getOverlordURL(), StringUtils.urlEncode(dataSource)
    );
    StatusResponseHolder response = httpClient.go(new Request(HttpMethod.DELETE, new URL(url)), responseHandler).get();

    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while deleting compaction config status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
  }

  /**
   * For all purposes, use the new APIs {@link #getClusterConfig()} or
   * {@link #getAllCompactionConfigs()}.
   */
  @Deprecated
  public DruidCompactionConfig getCoordinatorCompactionConfig() throws Exception
  {
    String url = StringUtils.format("%sconfig/compaction", getCoordinatorURL());
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.GET, new URL(url)), responseHandler
    ).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while getting compaction config status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
    return jsonMapper.readValue(response.getContent(), new TypeReference<>() {});
  }

  public List<DataSourceCompactionConfig> getAllCompactionConfigs() throws Exception
  {
    String url = StringUtils.format("%s/compaction/config/datasources", getOverlordURL());
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.GET, new URL(url)), responseHandler
    ).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while getting compaction config status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
    final CompactionConfigsResponse payload = jsonMapper.readValue(
        response.getContent(),
        new TypeReference<>() {}
    );
    return payload.getCompactionConfigs();
  }

  public DataSourceCompactionConfig getDataSourceCompactionConfig(String dataSource) throws Exception
  {
    String url = StringUtils.format(
        "%s/compaction/config/datasources/%s",
        getOverlordURL(), StringUtils.urlEncode(dataSource)
    );
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.GET, new URL(url)), responseHandler
    ).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while getting compaction config status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
    return jsonMapper.readValue(response.getContent(), new TypeReference<>() {});
  }

  public void forceTriggerAutoCompaction() throws Exception
  {
    // Perform a dummy update of task slots to force the coordinator to refresh its compaction config
    final ClusterCompactionConfig clusterConfig = getClusterConfig();
    updateCompactionTaskSlot(
        clusterConfig.getCompactionTaskSlotRatio(),
        clusterConfig.getMaxCompactionTaskSlots() + 10
    );
    updateCompactionTaskSlot(
        clusterConfig.getCompactionTaskSlotRatio(),
        clusterConfig.getMaxCompactionTaskSlots()
    );
    CompactionSimulateResult simulateResult = simulateRunOnCoordinator();
    log.info(
        "Before triggering compaction duty on Coordinator, expected jobs are %s",
        simulateResult.getCompactionStates()
    );

    String url = StringUtils.format("%scompaction/compact", getCoordinatorURL());
    StatusResponseHolder response = httpClient.go(new Request(HttpMethod.POST, new URL(url)), responseHandler).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while force trigger auto compaction status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }

    final CompactionTriggerResponse result = jsonMapper.readValue(response.getContent(), new TypeReference<>() {});
    log.info("Triggered compaction. Submitted tasks[%s].", result.getSubmittedTaskIds());

    simulateResult = simulateRunOnCoordinator();
    log.info(
        "After triggering compaction duty on Coordinator, expected jobs are %s",
        simulateResult.getCompactionStates()
    );
  }

  public void updateClusterConfig(ClusterCompactionConfig config) throws Exception
  {
    final String url = StringUtils.format(
        "%s/compaction/config/cluster",
        getOverlordURL()
    );
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.POST, new URL(url)).setContent(
            "application/json",
            jsonMapper.writeValueAsBytes(config)
        ),
        responseHandler
    ).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while updating cluster compaction config, status[%s], content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
  }

  public ClusterCompactionConfig getClusterConfig() throws Exception
  {
    String url = StringUtils.format("%s/compaction/config/cluster", getOverlordURL());
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.GET, new URL(url)), responseHandler
    ).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while getting compaction config status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
    return jsonMapper.readValue(response.getContent(), new TypeReference<>() {});
  }

  /**
   * This API is used only to force the coordinator to refresh its config.
   * For all other purposes, use {@link #updateClusterConfig}.
   */
  @Deprecated
  private void updateCompactionTaskSlot(Double compactionTaskSlotRatio, Integer maxCompactionTaskSlots) throws Exception
  {
    final String url = StringUtils.format(
        "%sconfig/compaction/taskslots?ratio=%s&max=%s",
        getCoordinatorURL(),
        StringUtils.urlEncode(compactionTaskSlotRatio.toString()),
        StringUtils.urlEncode(maxCompactionTaskSlots.toString())
    );
    StatusResponseHolder response = httpClient.go(new Request(HttpMethod.POST, new URL(url)), responseHandler).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while updating compaction task slot status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }

    // Verify that coordinator has the latest config now
    final DruidCompactionConfig configOnCoordinator = getCoordinatorCompactionConfig();
    Assert.assertEquals(
        maxCompactionTaskSlots,
        Integer.valueOf(configOnCoordinator.getMaxCompactionTaskSlots()),
        "Could not update compaction task slots on the Coordinator"
    );
    Assert.assertEquals(
        compactionTaskSlotRatio,
        Double.valueOf(configOnCoordinator.getCompactionTaskSlotRatio()),
        "Could not update compaction task slot ratio on the Coordinator"
    );
  }

  public Map<String, String> getCompactionProgress(String dataSource) throws Exception
  {
    String url = StringUtils.format("%scompaction/progress?dataSource=%s", getCoordinatorURL(), StringUtils.urlEncode(dataSource));
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.GET, new URL(url)), responseHandler
    ).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while getting compaction progress status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
    return jsonMapper.readValue(response.getContent(), new TypeReference<>() {});
  }

  public AutoCompactionSnapshot getCompactionStatus(String dataSource) throws Exception
  {
    String url = StringUtils.format("%scompaction/status?dataSource=%s", getCoordinatorURL(), StringUtils.urlEncode(dataSource));
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.GET, new URL(url)), responseHandler
    ).get();
    if (response.getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
      return null;
    } else if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while getting compaction status status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
    final CompactionStatusResponse latestSnapshots = jsonMapper.readValue(response.getContent(), new TypeReference<>() {});
    return latestSnapshots.getLatestStatus().get(0);
  }

  public CompactionSimulateResult simulateRunOnCoordinator() throws Exception
  {
    final ClusterCompactionConfig clusterConfig = getClusterConfig();

    final String url = StringUtils.format("%scompaction/simulate", getCoordinatorURL());
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.POST, new URL(url)).setContent(
            "application/json",
            jsonMapper.writeValueAsBytes(clusterConfig)
        ),
        responseHandler
    ).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while running simulation on Coordinator: status[%s], content[%s]",
          response.getStatus(), response.getContent()
      );
    }

    return jsonMapper.readValue(response.getContent(), new TypeReference<>() {});
  }
}
