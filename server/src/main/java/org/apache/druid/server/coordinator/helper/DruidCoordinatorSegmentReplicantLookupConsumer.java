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

package org.apache.druid.server.coordinator.helper;

import com.google.common.collect.Sets;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadQueuePeon;
import org.apache.druid.server.coordinator.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.SegmentReplicantLookup;
import org.apache.druid.server.coordinator.ServerHolder;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class DruidCoordinatorSegmentReplicantLookupConsumer implements DruidCoordinatorHelper, Consumer<SegmentReplicantLookup>
{
  
  private static final EmittingLogger log = new EmittingLogger(DruidCoordinatorSegmentReplicantLookupConsumer.class);
  
  private final Supplier<DruidCoordinatorContext> druidCoordinatorContextSupplier;
  
  public DruidCoordinatorSegmentReplicantLookupConsumer(
      Supplier<DruidCoordinatorContext> druidCoordinatorContextSupplier)
  {
    this.druidCoordinatorContextSupplier = druidCoordinatorContextSupplier;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    DruidCoordinatorContext druidCoordinatorContext = druidCoordinatorContextSupplier.get();
    ServerInventoryView serverInventoryView = druidCoordinatorContext.getServerInventoryView();
    Map<String, LoadQueuePeon> loadManagementPeons = druidCoordinatorContext.getLoadManagementPeons();
    LoadQueueTaskMaster taskMaster = druidCoordinatorContext.getTaskMaster();
    MetadataRuleManager metadataRuleManager = druidCoordinatorContext.getMetadataRuleManager();
        
    List<ImmutableDruidServer> servers = serverInventoryView
        .getInventory()
        .stream()
        .filter(DruidServer::segmentReplicatable)
        .map(DruidServer::toImmutableDruidServer)
        .collect(Collectors.toList());

    if (log.isDebugEnabled()) {
      // Display info about all historical servers
      log.debug("Servers");
      for (ImmutableDruidServer druidServer : servers) {
        log.debug("  %s", druidServer);
        log.debug("    -- DataSources");
        for (ImmutableDruidDataSource druidDataSource : druidServer.getDataSources()) {
          log.debug("    %s", druidDataSource);
        }
      }
    }

    // Find all historical servers, group them by subType and sort by ascending usage
    Set<String> decommissioningServers = params.getCoordinatorDynamicConfig().getDecommissioningNodes();
    final DruidCluster cluster = new DruidCluster();
    for (ImmutableDruidServer server : servers) {
      if (!loadManagementPeons.containsKey(server.getName())) {
        LoadQueuePeon loadQueuePeon = taskMaster.giveMePeon(server);
        loadQueuePeon.start();
        log.info("Created LoadQueuePeon for server[%s].", server.getName());

        loadManagementPeons.put(server.getName(), loadQueuePeon);
      }

      cluster.add(
          new ServerHolder(
              server,
              loadManagementPeons.get(server.getName()),
              decommissioningServers.contains(server.getHost())
          )
      );
    }

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(cluster);
    accept(segmentReplicantLookup);
    
    // Stop peons for servers that aren't there anymore.
    final Set<String> disappeared = Sets.newHashSet(loadManagementPeons.keySet());
    for (ImmutableDruidServer server : servers) {
      disappeared.remove(server.getName());
    }
    for (String name : disappeared) {
      log.info("Removing listener for server[%s] which is no longer there.", name);
      LoadQueuePeon peon = loadManagementPeons.remove(name);
      peon.stop();
    }

    return params.buildFromExisting()
                 .withDruidCluster(cluster)
                 .withDatabaseRuleManager(metadataRuleManager)
                 .withLoadManagementPeons(loadManagementPeons)
                 .withSegmentReplicantLookup(segmentReplicantLookup)
                 .withBalancerReferenceTimestamp(DateTimes.nowUtc())
                 .build();
  }

  public static class DruidCoordinatorContext
  {

    private final ServerInventoryView serverInventoryView;
    private final Map<String, LoadQueuePeon> loadManagementPeons;
    private final LoadQueueTaskMaster taskMaster;
    private final MetadataRuleManager metadataRuleManager;

    public DruidCoordinatorContext(ServerInventoryView serverInventoryView,
        Map<String, LoadQueuePeon> loadManagementPeons, LoadQueueTaskMaster taskMaster,
        MetadataRuleManager metadataRuleManager)
    {
      this.serverInventoryView = serverInventoryView;
      this.loadManagementPeons = loadManagementPeons;
      this.taskMaster = taskMaster;
      this.metadataRuleManager = metadataRuleManager;
    }

    public ServerInventoryView getServerInventoryView()
    {
      return serverInventoryView;
    }

    public Map<String, LoadQueuePeon> getLoadManagementPeons()
    {
      return loadManagementPeons;
    }

    public LoadQueueTaskMaster getTaskMaster()
    {
      return taskMaster;
    }

    public MetadataRuleManager getMetadataRuleManager()
    {
      return metadataRuleManager;
    }

  }
  
}
