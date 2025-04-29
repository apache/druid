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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.config.Configs;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.testing.cluster.ClusterTestingTaskConfig;
import org.apache.druid.testing.cluster.overlord.FaultyLagAggregator;
import org.apache.druid.testing.cluster.overlord.FaultyMetadataStorageCoordinator;
import org.apache.druid.testing.cluster.task.FaultyCoordinatorClient;
import org.apache.druid.testing.cluster.task.FaultyOverlordClient;
import org.apache.druid.testing.cluster.task.FaultyRemoteTaskActionClientFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Module that injects faulty clients into the Peon process to simulate various
 * fault scenarios.
 */
public class ClusterTestingModule implements DruidModule
{
  private static final Logger log = new Logger(ClusterTestingModule.class);

  private Set<NodeRole> roles;
  private boolean isClusterTestingEnabled = false;

  @Inject
  public void configure(
      Properties props,
      @Self Set<NodeRole> roles
  )
  {
    this.isClusterTestingEnabled = Boolean.parseBoolean(
        props.getProperty("druid.unsafe.cluster.testing", "false")
    );
    this.roles = roles;
  }

  @Override
  public void configure(Binder binder)
  {
    if (!isClusterTestingEnabled) {
      // Do nothing
      return;
    }

    if (roles.equals(Set.of(NodeRole.PEON))) {
      // If this is a Peon, bind faulty clients for Coordinator, Overlord and task actions
      binder.bind(ClusterTestingTaskConfig.class)
            .toProvider(TestConfigProvider.class)
            .in(LazySingleton.class);

      binder.bind(CoordinatorClient.class)
            .to(FaultyCoordinatorClient.class)
            .in(LazySingleton.class);
      binder.bind(OverlordClient.class)
            .to(FaultyOverlordClient.class)
            .in(LazySingleton.class);
      binder.bind(RemoteTaskActionClientFactory.class)
            .to(FaultyRemoteTaskActionClientFactory.class)
            .in(LazySingleton.class);
    } else if (roles.contains(NodeRole.OVERLORD)) {
      // If this is the Overlord, bind a faulty storage coordinator
      log.info("Running Overlord in cluster testing mode.");
      binder.bind(IndexerSQLMetadataStorageCoordinator.class)
            .to(FaultyMetadataStorageCoordinator.class)
            .in(ManageLifecycle.class);
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return List.of(
        new SimpleModule(getClass().getSimpleName())
            .registerSubtypes(new NamedType(FaultyLagAggregator.class, "unsafe_cluster_testing"))
    );
  }

  private static class TestConfigProvider implements Provider<ClusterTestingTaskConfig>
  {
    private final Task task;
    private final ObjectMapper mapper;

    @Inject
    public TestConfigProvider(Task task, ObjectMapper mapper)
    {
      this.task = task;
      this.mapper = mapper;
    }


    @Override
    public ClusterTestingTaskConfig get()
    {
      try {
        final Map<String, Object> configAsMap = task.getContextValue("clusterTesting");
        final String json = mapper.writeValueAsString(configAsMap);
        final ClusterTestingTaskConfig testingConfig = mapper.readValue(json, ClusterTestingTaskConfig.class);
        log.info("Running task in cluster testing mode with config[%s].", testingConfig);

        return Configs.valueOrDefault(testingConfig, new ClusterTestingTaskConfig(null, null, null));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
