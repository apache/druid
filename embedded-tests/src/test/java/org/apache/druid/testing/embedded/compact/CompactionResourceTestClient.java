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
import org.apache.druid.indexing.overlord.http.CompactionConfigsResponse;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.compaction.CompactionSimulateResult;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.http.ServletResourceUtils;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedServiceClient;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The methods in this class should eventually be updated to use
 * {@code CoordinatorClient} or {@code OverlordClient} as applicable.
 */
public class CompactionResourceTestClient
{
  private static final Logger log = new Logger(CompactionResourceTestClient.class);

  private final EmbeddedServiceClient client;

  CompactionResourceTestClient(EmbeddedDruidCluster cluster)
  {
    this.client = cluster.callApi().serviceClient();
  }

  private String getCoordinatorURL()
  {
    return "/druid/coordinator/v1";
  }

  private String getOverlordURL()
  {
    return "/druid/indexer/v1";
  }

  public void submitCompactionConfig(final DataSourceCompactionConfig dataSourceCompactionConfig)
  {
    final String dataSource = dataSourceCompactionConfig.getDataSource();
    String url = StringUtils.format(
        "%s/compaction/config/datasources/%s",
        getOverlordURL(), StringUtils.urlEncode(dataSource)
    );
    client.onLeaderOverlord(
        mapper -> new RequestBuilder(HttpMethod.POST, url).jsonContent(
            mapper,
            dataSourceCompactionConfig
        ),
        null
    );
    log.info("Submitted compaction config for datasource[%s]", dataSource);
  }

  public void deleteDataSourceCompactionConfig(final String dataSource)
  {
    String url = StringUtils.format(
        "%s/compaction/config/datasources/%s",
        getOverlordURL(), StringUtils.urlEncode(dataSource)
    );
    client.onLeaderOverlord(mapper -> new RequestBuilder(HttpMethod.DELETE, url), null);
  }

  public List<DataSourceCompactionConfig> getAllCompactionConfigs()
  {
    String url = StringUtils.format("%s/compaction/config/datasources", getOverlordURL());
    final CompactionConfigsResponse payload = client.onLeaderOverlord(
        mapper -> new RequestBuilder(HttpMethod.GET, url),
        new TypeReference<>() {}
    );
    return Objects.requireNonNull(payload).getCompactionConfigs();
  }

  public DataSourceCompactionConfig getDataSourceCompactionConfig(String dataSource)
  {
    String url = StringUtils.format(
        "%s/compaction/config/datasources/%s",
        getOverlordURL(), StringUtils.urlEncode(dataSource)
    );
    return client.onLeaderOverlord(
        mapper -> new RequestBuilder(HttpMethod.GET, url),
        new TypeReference<>() {}
    );
  }

  public void forceTriggerAutoCompaction()
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

    String url = StringUtils.format("%s/compaction/compact", getCoordinatorURL());
    client.onLeaderCoordinator(mapper -> new RequestBuilder(HttpMethod.POST, url), null);
  }

  public void updateClusterConfig(ClusterCompactionConfig config)
  {
    client.onLeaderOverlord(o -> o.updateClusterCompactionConfig(config));
  }

  public ClusterCompactionConfig getClusterConfig()
  {
    return client.onLeaderOverlord(OverlordClient::getClusterCompactionConfig);
  }

  /**
   * This API is currently only to force the coordinator to refresh its config.
   * For all other purposes, use {@link #updateClusterConfig}.
   */
  @Deprecated
  private void updateCompactionTaskSlot(Double compactionTaskSlotRatio, Integer maxCompactionTaskSlots)
  {
    final String url = StringUtils.format(
        "%s/config/compaction/taskslots?ratio=%s&max=%s",
        getCoordinatorURL(),
        StringUtils.urlEncode(compactionTaskSlotRatio.toString()),
        StringUtils.urlEncode(maxCompactionTaskSlots.toString())
    );
    client.onLeaderCoordinator(mapper -> new RequestBuilder(HttpMethod.POST, url), null);
  }

  public Map<String, String> getCompactionProgress(String dataSource)
  {
    String url = StringUtils.format(
        "%s/compaction/progress?dataSource=%s",
        getCoordinatorURL(),
        StringUtils.urlEncode(dataSource)
    );
    return client.onLeaderCoordinator(
        mapper -> new RequestBuilder(HttpMethod.GET, url),
        new TypeReference<>() {}
    );
  }

  public AutoCompactionSnapshot getCompactionStatus(String dataSource)
  {
    String url = StringUtils.format(
        "%s/compaction/status?dataSource=%s",
        getCoordinatorURL(),
        StringUtils.urlEncode(dataSource)
    );

    try {
      final CompactionStatusResponse latestSnapshots = client.onLeaderCoordinator(
          mapper -> new RequestBuilder(HttpMethod.GET, url),
          new TypeReference<>() {}
      );
      return Objects.requireNonNull(latestSnapshots).getLatestStatus().get(0);
    }
    catch (Exception e) {
      return ServletResourceUtils.getDefaultValueIfCauseIs404ElseThrow(e, () -> null);
    }
  }

  public CompactionSimulateResult simulateRunOnCoordinator()
  {
    final ClusterCompactionConfig clusterConfig = getClusterConfig();

    final String url = StringUtils.format("%s/compaction/simulate", getCoordinatorURL());
    return client.onLeaderCoordinator(
        mapper -> new RequestBuilder(HttpMethod.POST, url).jsonContent(mapper, clusterConfig),
        new TypeReference<>() {}
    );
  }
}
