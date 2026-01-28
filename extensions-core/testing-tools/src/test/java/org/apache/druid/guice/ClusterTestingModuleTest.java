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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.druid.cli.CliIndexer;
import org.apache.druid.cli.CliOverlord;
import org.apache.druid.cli.CliPeon;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.CoordinatorClientImpl;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.rpc.indexing.OverlordClientImpl;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.cluster.ClusterTestingTaskConfig;
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
  private static final ObjectMapper MAPPER = TestHelper
      .makeJsonMapper()
      .registerModules(new IndexingServiceTuningConfigModule().getJacksonModules())
      .registerModules(new IndexingServiceInputSourceModule().getJacksonModules());

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

      TaskActionClientFactory taskActionClientFactory = peonInjector.getInstance(TaskActionClientFactory.class);
      Assert.assertTrue(taskActionClientFactory instanceof FaultyRemoteTaskActionClientFactory);
    }
    finally {
      System.clearProperty("druid.unsafe.cluster.testing");
    }
  }

  @Test
  public void test_peonRunnable_getsConfigParams_ifProvidedInTaskContext() throws IOException
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
      final String taskJson = MAPPER.writeValueAsString(task);
      File file = temporaryFolder.newFile("task.json");
      FileUtils.write(file, taskJson, StandardCharsets.UTF_8);
      peon.taskAndStatusFile = List.of(file.getParent(), "1");

      final Injector baseInjector = new StartupInjectorBuilder().forServer().build();
      baseInjector.injectMembers(peon);

      final Injector peonInjector = peon.makeInjector(Set.of(NodeRole.PEON));

      final ClusterTestingTaskConfig taskConfig = peonInjector.getInstance(ClusterTestingTaskConfig.class);
      verifyTestingConfig(taskConfig);
    }
    finally {
      System.clearProperty("druid.unsafe.cluster.testing");
    }
  }

  @Test
  public void test_parallelIndexSupervisorTask_withDruidInputSource_hasNoCircularDeps() throws IOException
  {
    try {
      final CliPeon peon = new CliPeon();
      System.setProperty("druid.unsafe.cluster.testing", "true");

      // Create a ParallelIndexSupervisorTask
      final IndexIO indexIO = new IndexIO(MAPPER, ColumnConfig.DEFAULT);
      final DruidInputSource inputSource = new DruidInputSource(
          "test",
          Intervals.ETERNITY,
          null,
          TrueDimFilter.instance(),
          null,
          null,
          indexIO,
          new NoopCoordinatorClient(),
          new SegmentCacheManagerFactory(indexIO, MAPPER),
          new TaskConfigBuilder().build()
      );
      final ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
          inputSource,
          new JsonInputFormat(null, null, null, null, null),
          false,
          null
      );
      final Task task = new ParallelIndexSupervisorTask(
          "test-task",
          null,
          null,
          new ParallelIndexIngestionSpec(
              DataSchema.builder().withDataSource("test").build(),
              ioConfig,
              ParallelIndexTuningConfig.defaultConfig()
          ),
          Map.of("clusterTesting", createClusterTestingConfigMap())
      );

      // Write out the task payload in a temporary json file
      final String taskJson = MAPPER.writeValueAsString(task);
      File file = temporaryFolder.newFile("task.json");
      FileUtils.write(file, taskJson, StandardCharsets.UTF_8);
      peon.taskAndStatusFile = List.of(file.getParent(), "1");

      final Injector baseInjector = new StartupInjectorBuilder().forServer().build();
      baseInjector.injectMembers(peon);

      final Injector peonInjector = peon.makeInjector(Set.of(NodeRole.PEON));

      final ClusterTestingTaskConfig taskConfig = peonInjector.getInstance(ClusterTestingTaskConfig.class);
      verifyTestingConfig(taskConfig);
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

      GlobalTaskLockbox taskLockbox = overlordInjector.getInstance(GlobalTaskLockbox.class);
      Assert.assertTrue(taskLockbox instanceof FaultyTaskLockbox);
    }
    finally {
      System.clearProperty("druid.unsafe.cluster.testing");
    }
  }

  @Test
  public void test_indexerService_hasFaultyOverlordClient_ifTestingIsEnabled()
  {
    try {
      final CliIndexer indexer = new CliIndexer();
      System.setProperty("druid.unsafe.cluster.testing", "true");

      final Injector baseInjector = new StartupInjectorBuilder().forServer().build();
      baseInjector.injectMembers(indexer);

      final Injector indexerInjector = indexer.makeInjector(Set.of(NodeRole.INDEXER));

      OverlordClient overlordClient = indexerInjector.getInstance(OverlordClient.class);
      Assert.assertTrue(overlordClient instanceof FaultyOverlordClient);
    }
    finally {
      System.clearProperty("druid.unsafe.cluster.testing");
    }
  }

  private static void verifyTestingConfig(ClusterTestingTaskConfig taskConfig)
  {
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

  private Map<String, Object> createClusterTestingConfigMap()
  {
    return Map.of(
        "coordinatorClientConfig", Map.of("minSegmentHandoffDelay", "PT30S"),
        "taskActionClientConfig", Map.of("segmentPublishDelay", "PT10S", "segmentAllocateDelay", "PT5S"),
        "metadataConfig", Map.of("cleanupPendingSegments", false)
    );
  }
}
