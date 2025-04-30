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

import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.druid.cli.CliOverlord;
import org.apache.druid.cli.CliPeon;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.CoordinatorClientImpl;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.rpc.indexing.OverlordClientImpl;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.cluster.ClusterTestingTaskConfig;
import org.apache.druid.testing.cluster.overlord.FaultyMetadataStorageCoordinator;
import org.apache.druid.testing.cluster.overlord.FaultyTaskLockbox;
import org.apache.druid.testing.cluster.task.FaultyCoordinatorClient;
import org.apache.druid.testing.cluster.task.FaultyOverlordClient;
import org.apache.druid.testing.cluster.task.FaultyRemoteTaskActionClientFactory;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterTestingModuleTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void test_peonRunnable_isNotModified_ifTestingIsDisabled() throws IOException
  {
    try {
      final CliPeon peon = new CliPeon();
      System.setProperty("druid.unsafe.cluster.testing", "false");

      // Write out the task payload in a temporary json file
      File file = temporaryFolder.newFile("task.json");
      FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);
      peon.taskAndStatusFile = List.of(file.getParent(), "1");

      final Injector baseInjector = new StartupInjectorBuilder().forServer().build();
      baseInjector.injectMembers(peon);

      final Injector peonInjector = peon.makeInjector(Set.of(NodeRole.PEON));

      CoordinatorClient coordinatorClient = peonInjector.getInstance(CoordinatorClient.class);
      Assert.assertTrue(coordinatorClient instanceof CoordinatorClientImpl);

      OverlordClient overlordClient = peonInjector.getInstance(OverlordClient.class);
      Assert.assertTrue(overlordClient instanceof OverlordClientImpl);

      TaskActionClientFactory taskActionClientFactory = peonInjector.getInstance(TaskActionClientFactory.class);
      Assert.assertTrue(taskActionClientFactory instanceof RemoteTaskActionClientFactory);
    }
    finally {
      System.clearProperty("druid.unsafe.cluster.testing");
    }
  }

  @Test
  public void test_peonRunnable_hasFaultyClients_ifTestingIsEnabled() throws IOException
  {
    try {
      final CliPeon peon = new CliPeon();
      System.setProperty("druid.unsafe.cluster.testing", "true");

      // Write out the task payload in a temporary json file
      File file = temporaryFolder.newFile("task.json");
      FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);
      peon.taskAndStatusFile = List.of(file.getParent(), "1");

      final Injector baseInjector = new StartupInjectorBuilder().forServer().build();
      baseInjector.injectMembers(peon);

      final Injector peonInjector = peon.makeInjector(Set.of(NodeRole.PEON));

      CoordinatorClient coordinatorClient = peonInjector.getInstance(CoordinatorClient.class);
      Assert.assertTrue(coordinatorClient instanceof FaultyCoordinatorClient);

      OverlordClient overlordClient = peonInjector.getInstance(OverlordClient.class);
      Assert.assertTrue(overlordClient instanceof FaultyOverlordClient);

      TaskActionClientFactory taskActionClientFactory = peonInjector.getInstance(TaskActionClientFactory.class);
      Assert.assertTrue(taskActionClientFactory instanceof FaultyRemoteTaskActionClientFactory);
    }
    finally {
      System.clearProperty("druid.unsafe.cluster.testing");
    }
  }

  @Test
  public void test_peonRunnable_hasFaultParams_ifProvidedInTaskContext() throws IOException
  {
    try {
      final CliPeon peon = new CliPeon();
      System.setProperty("druid.unsafe.cluster.testing", "true");

      final Task task = new NoopTask(
          null,
          null,
          null,
          0L,
          0L,
          Map.of("clusterTesting", createClusterTestingConfigMap())
      );

      // Write out the task payload in a temporary json file
      final String taskJson = TestHelper.JSON_MAPPER.writeValueAsString(task);
      File file = temporaryFolder.newFile("task.json");
      FileUtils.write(file, taskJson, StandardCharsets.UTF_8);
      peon.taskAndStatusFile = List.of(file.getParent(), "1");

      final Injector baseInjector = new StartupInjectorBuilder().forServer().build();
      baseInjector.injectMembers(peon);

      final Injector peonInjector = peon.makeInjector(Set.of(NodeRole.PEON));

      final ClusterTestingTaskConfig taskConfig = peonInjector.getInstance(ClusterTestingTaskConfig.class);
      Assert.assertNotNull(taskConfig);
      Assert.assertNotNull(taskConfig.getCoordinatorClientConfig());
      Assert.assertNotNull(taskConfig.getOverlordClientConfig());
      Assert.assertNotNull(taskConfig.getTaskActionClientConfig());
      Assert.assertNotNull(taskConfig.getMetadataConfig());

      Assert.assertEquals(
          Duration.standardSeconds(10),
          taskConfig.getTaskActionClientConfig().getSegmentPublishDelay()
      );
      Assert.assertEquals(
          Duration.standardSeconds(5),
          taskConfig.getTaskActionClientConfig().getSegmentAllocateDelay()
      );
      Assert.assertEquals(
          Duration.standardSeconds(30),
          taskConfig.getCoordinatorClientConfig().getMinSegmentHandoffDelay()
      );
      Assert.assertFalse(
          taskConfig.getMetadataConfig().isCleanupPendingSegments()
      );
    }
    finally {
      System.clearProperty("druid.unsafe.cluster.testing");
    }
  }

  @Test
  public void test_overlordService_hasFaultyStorageCoordinator_ifTestingIsEnabled()
  {
    try {
      final CliOverlord overlord = new CliOverlord();
      System.setProperty("druid.unsafe.cluster.testing", "true");

      final Injector baseInjector = new StartupInjectorBuilder().forServer().build();
      baseInjector.injectMembers(overlord);

      final Injector overlordInjector = overlord.makeInjector(Set.of(NodeRole.OVERLORD));

      IndexerMetadataStorageCoordinator storageCoordinator =
          overlordInjector.getInstance(IndexerMetadataStorageCoordinator.class);
      Assert.assertTrue(storageCoordinator instanceof FaultyMetadataStorageCoordinator);

      TaskLockbox taskLockbox = overlordInjector.getInstance(TaskLockbox.class);
      Assert.assertTrue(taskLockbox instanceof FaultyTaskLockbox);
    }
    finally {
      System.clearProperty("druid.unsafe.cluster.testing");
    }
  }

  private Map<String, Object> createClusterTestingConfigMap()
  {
    final Map<String, Object> taskActionClientConfig = Map.of(
        "segmentPublishDelay", "PT10S",
        "segmentAllocateDelay", "PT5S"
    );
    final Map<String, Object> coordinatorClientConfig = Map.of(
        "minSegmentHandoffDelay", "PT30S"
    );
    final Map<String, Object> metadataConfig = Map.of(
        "cleanupPendingSegments", false
    );
    return Map.of(
        "coordinatorClientConfig", coordinatorClientConfig,
        "taskActionClientConfig", taskActionClientConfig,
        "metadataConfig", metadataConfig
    );
  }
}
