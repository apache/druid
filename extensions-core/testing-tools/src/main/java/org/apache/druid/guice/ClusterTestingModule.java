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
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.testing.cluster.ClusterTestingTaskConfig;
import org.apache.druid.testing.cluster.overlord.FaultyLagAggregator;
import org.apache.druid.testing.cluster.overlord.FaultyTaskLockbox;
import org.apache.druid.testing.cluster.task.FaultyCoordinatorClient;
import org.apache.druid.testing.cluster.task.FaultyOverlordClient;
import org.apache.druid.testing.cluster.task.FaultyRemoteTaskActionClientFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Module that injects faulty clients into the Peon process to simulate various
 * fault scenarios.
 */
public class ClusterTestingModule implements DruidModule
{
  private static final Logger log = new Logger(ClusterTestingModule.class);
  private static final String PROPERTY_ENABLE = "druid.unsafe.cluster.testing";
  private static final String PROPERTY_OVERLORD_CLIENT_CONFIG = "druid.unsafe.cluster.testing.overlordClient";

  private Set<NodeRole> roles;
  private boolean isClusterTestingEnabled = false;

  @Inject
  public void configure(
      Properties props,
      @Self Set<NodeRole> roles
  )
  {
    this.isClusterTestingEnabled = Boolean.parseBoolean(
        props.getProperty(PROPERTY_ENABLE, "false")
    );
    this.roles = roles;
  }

  @Override
  public void configure(Binder binder)
  {
    if (isClusterTestingEnabled) {
      log.warn(
          "Running service with roles[%s] in cluster testing mode. This is an unsafe test-only"
          + " mode and must never be used in a production cluster."
          + " Set property[%s=false] to disable testing mode.",
          roles, PROPERTY_ENABLE
      );
      bindDependenciesForClusterTestingMode(binder);
    } else {
      log.info("Cluster testing is disabled. Set property[%s=true] to enable it.", PROPERTY_ENABLE);
    }
  }

  private void bindDependenciesForClusterTestingMode(Binder binder)
  {
    if (roles.equals(Set.of(NodeRole.PEON))) {
      // Bind cluster testing config
      binder.bind(ClusterTestingTaskConfig.class)
            .toProvider(TestConfigProvider.class)
            .in(LazySingleton.class);

      // Bind faulty clients for Coordinator, Overlord and task actions
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
      binder.bind(GlobalTaskLockbox.class)
            .to(FaultyTaskLockbox.class)
            .in(LazySingleton.class);
    } else if (roles.contains(NodeRole.INDEXER)) {
      JsonConfigProvider.bind(binder, PROPERTY_OVERLORD_CLIENT_CONFIG, ClusterTestingTaskConfig.OverlordClientConfig.class);
      binder.bind(OverlordClient.class)
            .to(FaultyOverlordClient.class)
            .in(LazySingleton.class);
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return List.of(
        new SimpleModule(getClass().getSimpleName())
            .registerSubtypes(new NamedType(FaultyLagAggregator.class, "faulty"))
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
        final ClusterTestingTaskConfig testingConfig = ClusterTestingTaskConfig.forTask(task, mapper);
        log.warn("Running task in cluster testing mode with config[%s].", testingConfig);

        return testingConfig;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
