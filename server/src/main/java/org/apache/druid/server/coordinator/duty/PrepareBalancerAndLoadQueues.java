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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.balancer.BalancerStrategyFactory;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.SegmentLoadingConfig;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * This duty does the following:
 * <ul>
 *   <li>Creates an immutable {@link DruidCluster} consisting of {@link ServerHolder}s
 *   which represent the current state of the servers in the cluster.</li>
 *   <li>Starts and stops load peons for new and disappeared servers respectively.</li>
 *   <li>Cancels in-progress loads on all decommissioning servers. This is done
 *   here to ensure that under-replicated segments are assigned to active servers
 *   in the {@link RunRules} duty after this.</li>
 *   <li>Initializes the {@link BalancerStrategy} for the run.</li>
 * </ul>
 */
public class PrepareBalancerAndLoadQueues implements CoordinatorDuty
{
  private static final Logger log = new Logger(PrepareBalancerAndLoadQueues.class);

  private final LoadQueueTaskMaster taskMaster;
  private final SegmentLoadQueueManager loadQueueManager;
  private final ServerInventoryView serverInventoryView;
  private final BalancerStrategyFactory balancerStrategyFactory;

  public PrepareBalancerAndLoadQueues(
      LoadQueueTaskMaster taskMaster,
      SegmentLoadQueueManager loadQueueManager,
      BalancerStrategyFactory balancerStrategyFactory,
      ServerInventoryView serverInventoryView
  )
  {
    this.taskMaster = taskMaster;
    this.loadQueueManager = loadQueueManager;
    this.balancerStrategyFactory = balancerStrategyFactory;
    this.serverInventoryView = serverInventoryView;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    List<ImmutableDruidServer> currentServers = prepareCurrentServers();
    taskMaster.resetPeonsForNewServers(currentServers);

    final CoordinatorDynamicConfig dynamicConfig = params.getCoordinatorDynamicConfig();
    final SegmentLoadingConfig segmentLoadingConfig
        = SegmentLoadingConfig.create(dynamicConfig, params.getUsedSegments().size());

    final DruidCluster cluster = prepareCluster(dynamicConfig, segmentLoadingConfig, currentServers);
    cancelLoadsOnDecommissioningServers(cluster);

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    collectHistoricalStats(cluster, stats);
    collectUsedSegmentStats(params, stats);

    final int numBalancerThreads = segmentLoadingConfig.getBalancerComputeThreads();
    final BalancerStrategy balancerStrategy = balancerStrategyFactory.createBalancerStrategy(numBalancerThreads);
    log.info(
        "Using balancer strategy [%s] with [%d] threads.",
        balancerStrategy.getClass().getSimpleName(), numBalancerThreads
    );

    return params.buildFromExisting()
                 .withDruidCluster(cluster)
                 .withBalancerStrategy(balancerStrategy)
                 .withSegmentLoadingConfig(segmentLoadingConfig)
                 .withSegmentAssignerUsing(loadQueueManager)
                 .build();
  }

  /**
   * Cancels all load/move operations on decommissioning servers. This should
   * be done before initializing the SegmentReplicantLookup so that
   * under-replicated segments can be assigned in the current run itself.
   */
  private void cancelLoadsOnDecommissioningServers(DruidCluster cluster)
  {
    final AtomicInteger cancelledCount = new AtomicInteger(0);
    final List<ServerHolder> decommissioningServers
        = cluster.getAllServers().stream()
                 .filter(ServerHolder::isDecommissioning)
                 .collect(Collectors.toList());

    for (ServerHolder server : decommissioningServers) {
      server.getQueuedSegments().forEach(
          (segment, action) -> {
            // Cancel the operation if it is a type of load
            if (action.isLoad() && server.cancelOperation(action, segment)) {
              cancelledCount.incrementAndGet();
            }
          }
      );
    }

    if (cancelledCount.get() > 0) {
      log.info(
          "Cancelled [%d] load/move operations on [%d] decommissioning servers.",
          cancelledCount.get(), decommissioningServers.size()
      );
    }
  }

  private List<ImmutableDruidServer> prepareCurrentServers()
  {
    return serverInventoryView
        .getInventory()
        .stream()
        .filter(DruidServer::isSegmentReplicationOrBroadcastTarget)
        .map(DruidServer::toImmutableDruidServer)
        .collect(Collectors.toList());
  }

  private DruidCluster prepareCluster(
      CoordinatorDynamicConfig dynamicConfig,
      SegmentLoadingConfig segmentLoadingConfig,
      List<ImmutableDruidServer> currentServers
  )
  {
    final Set<String> decommissioningServers = dynamicConfig.getDecommissioningNodes();
    final DruidCluster.Builder cluster = DruidCluster.builder();
    for (ImmutableDruidServer server : currentServers) {
      cluster.add(
          new ServerHolder(
              server,
              taskMaster.getPeonForServer(server),
              decommissioningServers.contains(server.getHost()),
              segmentLoadingConfig.getMaxSegmentsInLoadQueue(),
              segmentLoadingConfig.getMaxLifetimeInLoadQueue()
          )
      );
    }
    return cluster.build();
  }

  private void collectHistoricalStats(DruidCluster cluster, CoordinatorRunStats stats)
  {
    cluster.getHistoricals().forEach((tier, historicals) -> {
      RowKey rowKey = RowKey.of(Dimension.TIER, tier);
      stats.add(Stats.Tier.HISTORICAL_COUNT, rowKey, historicals.size());

      long totalCapacity = historicals.stream().mapToLong(ServerHolder::getMaxSize).sum();
      stats.add(Stats.Tier.TOTAL_CAPACITY, rowKey, totalCapacity);
    });
  }

  private void collectUsedSegmentStats(DruidCoordinatorRuntimeParams params, CoordinatorRunStats stats)
  {
    params.getUsedSegmentsTimelinesPerDataSource().forEach((dataSource, timeline) -> {
      long totalSizeOfUsedSegments = timeline.iterateAllObjects().stream()
                                             .mapToLong(DataSegment::getSize).sum();

      RowKey datasourceKey = RowKey.of(Dimension.DATASOURCE, dataSource);
      stats.add(Stats.Segments.USED_BYTES, datasourceKey, totalSizeOfUsedSegments);
      stats.add(Stats.Segments.USED, datasourceKey, timeline.getNumObjects());
    });
  }
}
