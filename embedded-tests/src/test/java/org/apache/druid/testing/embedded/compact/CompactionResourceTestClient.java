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

package org.apache.druid.testing.embedded.compact;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.overlord.http.CompactionConfigsResponse;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.compaction.CompactionSimulateResult;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * The methods in this class should eventually be updated to use
 * {@code CoordinatorClient} or {@code OverlordClient} as applicable.
 */
public class CompactionResourceTestClient
{
  private static final Logger log = new Logger(CompactionResourceTestClient.class);

  private final ObjectMapper jsonMapper;
  private final EmbeddedOverlord overlord;
  private final EmbeddedCoordinator coordinator;
  private final StatusResponseHandler responseHandler;

  CompactionResourceTestClient(
      EmbeddedCoordinator coordinator,
      EmbeddedOverlord overlord
  )
  {
    this.jsonMapper = TestHelper.JSON_MAPPER
        .registerModules(new SketchModule().getJacksonModules())
        .registerModules(new HllSketchModule().getJacksonModules())
        .registerModules(new DoublesSketchModule().getJacksonModules());
    this.overlord = overlord;
    this.coordinator = coordinator;
    this.responseHandler = StatusResponseHandler.getInstance();
  }

  private HttpClient httpClient()
  {
    return overlord.bindings().escalatedHttpClient();
  }

  private String getCoordinatorURL()
  {
    return StringUtils.format(
        "http://%s:%s/druid/coordinator/v1/",
        coordinator.bindings().selfNode().getHost(),
        coordinator.bindings().selfNode().getPlaintextPort()
    );
  }

  private String getOverlordURL()
  {
    return StringUtils.format(
        "http://%s:%s/druid/indexer/v1",
        overlord.bindings().selfNode().getHost(),
        overlord.bindings().selfNode().getPlaintextPort()
    );
  }

  public void submitCompactionConfig(final DataSourceCompactionConfig dataSourceCompactionConfig) throws Exception
  {
    final String dataSource = dataSourceCompactionConfig.getDataSource();
    String url = StringUtils.format(
        "%s/compaction/config/datasources/%s",
        getOverlordURL(), StringUtils.urlEncode(dataSource)
    );
    StatusResponseHolder response = httpClient().go(
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
    StatusResponseHolder response = httpClient().go(new Request(HttpMethod.DELETE, new URL(url)), responseHandler).get();

    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while deleting compaction config status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
  }

  public List<DataSourceCompactionConfig> getAllCompactionConfigs() throws Exception
  {
    String url = StringUtils.format("%s/compaction/config/datasources", getOverlordURL());
    StatusResponseHolder response = httpClient().go(
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
    StatusResponseHolder response = httpClient().go(
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
    final CompactionSimulateResult simulateResult = simulateRunOnCoordinator();
    log.info(
        "Triggering compaction duty on Coordinator. Expected jobs: %s",
        simulateResult.getCompactionStates()
    );

    String url = StringUtils.format("%scompaction/compact", getCoordinatorURL());
    StatusResponseHolder response = httpClient().go(new Request(HttpMethod.POST, new URL(url)), responseHandler).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while force trigger auto compaction status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
  }

  public void updateClusterConfig(ClusterCompactionConfig config) throws Exception
  {
    final String url = StringUtils.format(
        "%s/compaction/config/cluster",
        getOverlordURL()
    );
    StatusResponseHolder response = httpClient().go(
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
    StatusResponseHolder response = httpClient().go(
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
   * This API is currently only to force the coordinator to refresh its config.
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
    StatusResponseHolder response = httpClient().go(new Request(HttpMethod.POST, new URL(url)), responseHandler).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while updating compaction task slot status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
  }

  public Map<String, String> getCompactionProgress(String dataSource) throws Exception
  {
    String url = StringUtils.format("%scompaction/progress?dataSource=%s", getCoordinatorURL(), StringUtils.urlEncode(dataSource));
    StatusResponseHolder response = httpClient().go(
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
    StatusResponseHolder response = httpClient().go(
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
    StatusResponseHolder response = httpClient().go(
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
