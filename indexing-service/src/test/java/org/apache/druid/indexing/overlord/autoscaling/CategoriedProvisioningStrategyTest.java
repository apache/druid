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

package org.apache.druid.indexing.overlord.autoscaling;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.guava.DSuppliers;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TestTasks;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.RemoteTaskRunner;
import org.apache.druid.indexing.overlord.ZkWorker;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.setup.CategoriedWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.FillCapacityWithCategorySpecWorkerSelectStrategy;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerCategorySpec;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class CategoriedProvisioningStrategyTest
{
  public static final String CATEGORY_1 = "category1";
  public static final String CATEGORY_2 = "category2";
  private AutoScaler autoScalerDefault;
  private AutoScaler autoScalerCategory1;
  private AutoScaler autoScalerCategory2;
  private final Map<String, AutoScaler> categoryAutoScaler = new HashMap<>();
  private Task testTask;
  private CategoriedProvisioningConfig config;
  private final ScheduledExecutorService executorService = Execs.scheduledSingleThreaded("test service");
  private static final String MIN_VERSION = "2014-01-00T00:01:00Z";
  private static final String INVALID_VERSION = "0";

  @Before
  public void setup()
  {
    autoScalerDefault = EasyMock.createMock(AutoScaler.class);

    autoScalerCategory1 = EasyMock.createMock(AutoScaler.class);
    autoScalerCategory2 = EasyMock.createMock(AutoScaler.class);

    categoryAutoScaler.clear();
    categoryAutoScaler.put(CATEGORY_1, autoScalerCategory1);
    categoryAutoScaler.put(CATEGORY_2, autoScalerCategory2);

    testTask = TestTasks.immediateSuccess("task1");

    config = new CategoriedProvisioningConfig()
        .setMaxScalingDuration(new Period(1000))
        .setNumEventsToTrack(10)
        .setPendingTaskTimeout(new Period(0))
        .setWorkerVersion(MIN_VERSION)
        .setMaxScalingStep(2);
  }

  @Test
  public void testDefaultAutoscalerSuccessfulInitialMinWorkers()
  {
    AtomicReference<WorkerBehaviorConfig> workerConfig = createWorkerConfigRef(false);
    WorkerCategorySpec workerCategorySpec = createWorkerCategorySpec(false);

    CategoriedProvisioningStrategy strategy = createStrategy(workerConfig, workerCategorySpec);
    setupAutoscaler(autoScalerDefault, 3, 5, Collections.emptyList());
    setupAutoscaler(autoScalerCategory1, 2, 4, Collections.emptyList());
    setupAutoscaler(autoScalerCategory2, 4, 6, Collections.emptyList());

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // No pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(Collections.emptyList());
    // No workers
    EasyMock.expect(runner.getWorkers()).andReturn(Collections.emptyList());

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig()).times(3);

    EasyMock.expect(autoScalerDefault.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("aNode"))
    ).times(3);

    EasyMock.expect(autoScalerCategory1.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category1Node"))
    ).times(2);

    EasyMock.expect(autoScalerCategory2.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category2Node"))
    ).times(4);

    EasyMock.replay(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    Assert.assertEquals(9, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }
  }

  @Test
  public void testDefaultAutoscalerDidntSpawnInitialMinWorkers()
  {
    AtomicReference<WorkerBehaviorConfig> workerConfig = createWorkerConfigRef(true);
    WorkerCategorySpec workerCategorySpec = createWorkerCategorySpec(false);

    CategoriedProvisioningStrategy strategy = createStrategy(workerConfig, workerCategorySpec);
    setupAutoscaler(autoScalerDefault, 3, 5, Collections.emptyList());
    setupAutoscaler(autoScalerCategory1, 2, 4, Collections.emptyList());
    setupAutoscaler(autoScalerCategory2, 4, 6, Collections.emptyList());

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // No pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(Collections.emptyList());
    // No workers
    EasyMock.expect(runner.getWorkers()).andReturn(Collections.emptyList());
    // Expect this call two times because each categorizied autoscaler will call it
    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig()).times(2);

    EasyMock.expect(autoScalerCategory1.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category1Node"))
    ).times(2);
    EasyMock.expect(autoScalerCategory2.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category2Node"))
    ).times(4);

    EasyMock.replay(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    Assert.assertEquals(6, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }
  }

  @Test
  public void testDefaultAutoscalerSuccessfulMinWorkers()
  {
    AtomicReference<WorkerBehaviorConfig> workerConfig = createWorkerConfigRef(false);
    WorkerCategorySpec workerCategorySpec = createWorkerCategorySpec(false);

    CategoriedProvisioningStrategy strategy = createStrategy(workerConfig, workerCategorySpec);
    setupAutoscaler(autoScalerDefault, 3, 5, Collections.emptyList());
    setupAutoscaler(autoScalerCategory1, 2, 4, Collections.emptyList());
    setupAutoscaler(autoScalerCategory2, 4, 6, Collections.emptyList());

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // No pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(Collections.emptyList());
    // 1 node already running, only provision 2 more.
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            new TestZkWorker(testTask).toImmutable()
        )
    );

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig());

    EasyMock.expect(autoScalerDefault.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("aNode"))
    ).times(2);

    EasyMock.replay(runner, autoScalerDefault);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    Assert.assertEquals(2, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }
  }

  @Test
  public void testDefaultAutoscalerDidntSpawnMinWorkers()
  {
    AtomicReference<WorkerBehaviorConfig> workerConfig = createWorkerConfigRef(true);
    WorkerCategorySpec workerCategorySpec = createWorkerCategorySpec(false);

    CategoriedProvisioningStrategy strategy = createStrategy(workerConfig, workerCategorySpec);
    setupAutoscaler(autoScalerDefault, 3, 5, Collections.emptyList());
    setupAutoscaler(autoScalerCategory1, 2, 4, Collections.emptyList());
    setupAutoscaler(autoScalerCategory2, 4, 6, Collections.emptyList());

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // No pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(Collections.emptyList());
    // 1 node already running, only provision 2 more.
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            new TestZkWorker(testTask).toImmutable()
        )
    );

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig());

    EasyMock.expect(autoScalerDefault.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("aNode"))
    ).times(2);

    EasyMock.replay(runner, autoScalerDefault);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertFalse(provisionedSomething);
    Assert.assertTrue(provisioner.getStats().toList().isEmpty());
  }

  @Test
  public void testCategoriedAutoscalerSpawnedMinWorkers()
  {
    AtomicReference<WorkerBehaviorConfig> workerConfig = createWorkerConfigRef(true);
    WorkerCategorySpec workerCategorySpec = createWorkerCategorySpec(false, "noop", CATEGORY_1, "", "");

    CategoriedProvisioningStrategy strategy = createStrategy(workerConfig, workerCategorySpec);
    setupAutoscaler(autoScalerDefault, 3, 5, Collections.emptyList());
    setupAutoscaler(autoScalerCategory1, 5, 7, Collections.emptyList());
    setupAutoscaler(autoScalerCategory2, 4, 6, Collections.emptyList());

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // One pending task
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(Collections.singletonList(NoopTask.create()));
    // No workers
    EasyMock.expect(runner.getWorkers()).andReturn(Collections.emptyList());

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig());

    EasyMock.expect(autoScalerCategory1.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category1Node"))
    ).times(5);

    EasyMock.replay(runner, autoScalerCategory1);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    Assert.assertEquals(5, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }
  }

  @Test
  public void testCategoriedAutoscalerSpawnedAdditionalWorker()
  {
    AtomicReference<WorkerBehaviorConfig> workerConfig = createWorkerConfigRef(true);
    WorkerCategorySpec workerCategorySpec = createWorkerCategorySpec(false, "noop", CATEGORY_1, "", "");

    CategoriedProvisioningStrategy strategy = createStrategy(workerConfig, workerCategorySpec);
    setupAutoscaler(autoScalerDefault, 3, 5, Collections.emptyList());
    setupAutoscaler(autoScalerCategory1, 2, 3, Collections.emptyList());
    setupAutoscaler(autoScalerCategory2, 4, 6, Collections.emptyList());

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // One pending task
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(Collections.singletonList(NoopTask.create()));
    // Min workers are running
    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new TestZkWorker(testTask, CATEGORY_1).toImmutable()
        )
    );

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig());

    EasyMock.expect(autoScalerCategory1.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category1Node"))
    );

    EasyMock.replay(runner, autoScalerCategory1);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    Assert.assertEquals(1, provisioner.getStats().toList().size());
    Assert.assertSame(provisioner.getStats().toList().get(0).getEvent(), ScalingStats.EVENT.PROVISION);
  }

  @Test
  public void testCategoriedAutoscalerSpawnedUpToMaxWorkers()
  {
    AtomicReference<WorkerBehaviorConfig> workerConfig = createWorkerConfigRef(true);
    WorkerCategorySpec workerCategorySpec = createWorkerCategorySpec(false, "noop", CATEGORY_1, "", "");

    CategoriedProvisioningStrategy strategy = createStrategy(workerConfig, workerCategorySpec);
    setupAutoscaler(autoScalerDefault, 3, 5, Collections.emptyList());
    setupAutoscaler(autoScalerCategory1, 2, 3, Collections.emptyList());
    setupAutoscaler(autoScalerCategory2, 4, 6, Collections.emptyList());

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // Two pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(Arrays.asList(NoopTask.create(), NoopTask.create()));
    // Min workers are running
    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new TestZkWorker(testTask, CATEGORY_1).toImmutable()
        )
    );

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig());

    EasyMock.expect(autoScalerCategory1.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category1Node"))
    );

    EasyMock.replay(runner, autoScalerCategory1);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    Assert.assertEquals(1, provisioner.getStats().toList().size());
    Assert.assertSame(provisioner.getStats().toList().get(0).getEvent(), ScalingStats.EVENT.PROVISION);
  }


  private void setupAutoscaler(AutoScaler autoScaler, int minWorkers, int maxWorkers, List<String> pendingTasks)
  {
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(minWorkers);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(maxWorkers);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(pendingTasks);
  }

  private WorkerCategorySpec createWorkerCategorySpec(boolean isStrong)
  {
    Map<String, WorkerCategorySpec.CategoryConfig> categoryMap = new HashMap<>();
    return new WorkerCategorySpec(categoryMap, isStrong);
  }

  private WorkerCategorySpec createWorkerCategorySpec(
      boolean isStrong,
      String taskType,
      String defaultCategory,
      String datasource,
      String category
  )
  {
    Map<String, String> categoryAffinity = new HashMap<>();
    categoryAffinity.put(datasource, category);
    WorkerCategorySpec.CategoryConfig categoryConfig = new WorkerCategorySpec.CategoryConfig(
        defaultCategory,
        categoryAffinity
    );
    Map<String, WorkerCategorySpec.CategoryConfig> categoryMap = new HashMap<>();
    categoryMap.put(taskType, categoryConfig);
    return new WorkerCategorySpec(categoryMap, isStrong);
  }

  private AtomicReference<WorkerBehaviorConfig> createWorkerConfigRef(boolean isStrong)
  {
    return new AtomicReference<>(
        new CategoriedWorkerBehaviorConfig(
            new FillCapacityWithCategorySpecWorkerSelectStrategy(null),
            isStrong ? null : autoScalerDefault,
            categoryAutoScaler
        )
    );
  }

  private CategoriedProvisioningStrategy createStrategy(
      AtomicReference<WorkerBehaviorConfig> workerConfigRef,
      WorkerCategorySpec workerCategorySpec
  )
  {
    return new CategoriedProvisioningStrategy(
        config,
        DSuppliers.of(workerConfigRef),
        workerCategorySpec,
        new ProvisioningSchedulerConfig(),
        () -> executorService
    );
  }

  public static class TestZkWorker extends ZkWorker
  {
    private final Task testTask;

    public TestZkWorker(
        Task testTask
    )
    {
      this(testTask, "http", "host", "ip", MIN_VERSION, WorkerConfig.DEFAULT_CATEGORY);
    }

    public TestZkWorker(
        Task testTask,
        String category
    )
    {
      this(testTask, "http", "host", "ip", MIN_VERSION, category);
    }

    public TestZkWorker(
        Task testTask,
        String scheme,
        String host,
        String ip,
        String version,
        String category
    )
    {
      this(testTask, scheme, host, ip, version, 1, category);
    }

    public TestZkWorker(
        Task testTask,
        String scheme,
        String host,
        String ip,
        String version,
        int capacity,
        String category
    )
    {
      super(new Worker(scheme, host, ip, capacity, version, category), null, new DefaultObjectMapper());

      this.testTask = testTask;
    }

    @Override
    public Map<String, TaskAnnouncement> getRunningTasks()
    {
      if (testTask == null) {
        return new HashMap<>();
      }
      return ImmutableMap.of(
          testTask.getId(),
          TaskAnnouncement.create(
              testTask,
              TaskStatus.running(testTask.getId()),
              TaskLocation.unknown()
          )
      );
    }
  }
}
